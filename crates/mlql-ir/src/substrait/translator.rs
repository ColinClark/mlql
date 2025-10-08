//! Core Substrait translator

use crate::{Program, Pipeline, Source, Operator, Expr, Value, BinOp, UnOp, ColumnRef, Projection, SortKey, AggCall, JoinType};
use super::schema::SchemaProvider;
use substrait::proto::Plan;
use std::cell::RefCell;
use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
pub enum TranslateError {
    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Unsupported operator: {0}")]
    UnsupportedOperator(String),

    #[error("Translation error: {0}")]
    Translation(String),
}

/// Function registry for tracking which Substrait functions are used
#[derive(Debug)]
struct FunctionRegistry {
    /// Map function name to anchor ID
    functions: HashMap<String, u32>,
    /// Next available anchor
    next_anchor: u32,
}

impl FunctionRegistry {
    fn new() -> Self {
        Self {
            functions: HashMap::new(),
            next_anchor: 1, // Start at 1 (0 is reserved)
        }
    }

    /// Register a function and get its anchor ID
    fn register(&mut self, function_name: &str) -> u32 {
        if let Some(&anchor) = self.functions.get(function_name) {
            return anchor;
        }

        let anchor = self.next_anchor;
        self.functions.insert(function_name.to_string(), anchor);
        self.next_anchor += 1;
        anchor
    }

    /// Get all registered functions
    fn get_functions(&self) -> Vec<(String, u32)> {
        let mut funcs: Vec<_> = self.functions.iter().map(|(k, v)| (k.clone(), *v)).collect();
        funcs.sort_by_key(|(_, anchor)| *anchor);
        funcs
    }
}

/// Translator for MLQL IR → Substrait protocol buffers.
///
/// Converts MLQL's JSON intermediate representation into Substrait plans that can be
/// executed by Substrait-compatible engines like DuckDB.
///
/// # Example
///
/// ```rust
/// use mlql_ir::{Program, Pipeline, Source};
/// use mlql_ir::substrait::{SubstraitTranslator, MockSchemaProvider, TableSchema, ColumnInfo};
/// use prost::Message;
///
/// // Set up schema
/// let mut schema_provider = MockSchemaProvider::new();
/// schema_provider.add_table(TableSchema {
///     name: "users".to_string(),
///     columns: vec![
///         ColumnInfo { name: "id".to_string(), data_type: "INTEGER".to_string(), nullable: true },
///         ColumnInfo { name: "name".to_string(), data_type: "VARCHAR".to_string(), nullable: true },
///     ],
/// });
///
/// // Create IR
/// let program = Program {
///     pragma: None,
///     lets: vec![],
///     pipeline: Pipeline {
///         source: Source::Table { name: "users".to_string(), alias: None },
///         ops: vec![],
///     },
/// };
///
/// // Translate to Substrait
/// let translator = SubstraitTranslator::new(&schema_provider);
/// let plan = translator.translate(&program).unwrap();
///
/// // Serialize for execution
/// let mut bytes = Vec::new();
/// plan.encode(&mut bytes).unwrap();
/// ```
///
/// # Schema Tracking
///
/// The translator tracks schema transformations through the pipeline:
/// - `from table` → initial table schema
/// - `select [a, b]` → projected columns `[a, b]`
/// - `group by key { agg: sum(x) }` → `[key, agg]`
/// - `join orders on id == order_id` → `[left_cols..., right_cols...]`
///
/// Schema tracking ensures correct field references throughout the plan.
///
/// # Operator Mapping
///
/// | MLQL Operator | Substrait Relation |
/// |---------------|-------------------|
/// | `from table` | `ReadRel` |
/// | `filter` | `FilterRel` |
/// | `select` | `ProjectRel` |
/// | `sort` | `SortRel` |
/// | `take` | `FetchRel` |
/// | `distinct` | `AggregateRel` (group by all) |
/// | `group by` | `AggregateRel` |
/// | `join` | `JoinRel` |
///
/// # Function Extensions
///
/// The translator automatically registers Substrait standard functions used in expressions:
/// - Comparison: `eq`, `ne`, `lt`, `gt`, `le`, `ge`
/// - Logical: `and`, `or`, `not`
/// - String: `like`, `ilike`
/// - Arithmetic: `add`, `subtract`, `multiply`, `divide`
/// - Aggregate: `sum` (more coming)
///
/// Functions are registered with unique anchor IDs and extension URIs as per Substrait spec.
pub struct SubstraitTranslator<'a> {
    schema_provider: &'a dyn SchemaProvider,
    /// Function registry (using RefCell for interior mutability)
    function_registry: RefCell<FunctionRegistry>,
}

impl<'a> SubstraitTranslator<'a> {
    /// Create a new translator with the given schema provider.
    ///
    /// The schema provider is used to look up table schemas (column names and types)
    /// during translation.
    pub fn new(schema_provider: &'a dyn SchemaProvider) -> Self {
        Self {
            schema_provider,
            function_registry: RefCell::new(FunctionRegistry::new()),
        }
    }

    /// Translate an MLQL IR Program to a Substrait Plan.
    ///
    /// # Arguments
    ///
    /// * `program` - The MLQL IR program to translate
    ///
    /// # Returns
    ///
    /// A Substrait `Plan` containing the translated relation tree, or a `TranslateError`
    /// if translation fails (e.g., unknown table, unsupported operator).
    ///
    /// # Example
    ///
    /// ```rust
    /// # use mlql_ir::{Program, Pipeline, Source};
    /// # use mlql_ir::substrait::{SubstraitTranslator, MockSchemaProvider, TableSchema, ColumnInfo};
    /// # let mut schema_provider = MockSchemaProvider::new();
    /// # schema_provider.add_table(TableSchema {
    /// #     name: "users".to_string(),
    /// #     columns: vec![
    /// #         ColumnInfo { name: "id".to_string(), data_type: "INTEGER".to_string(), nullable: true },
    /// #     ],
    /// # });
    /// # let program = Program {
    /// #     pragma: None,
    /// #     lets: vec![],
    /// #     pipeline: Pipeline {
    /// #         source: Source::Table { name: "users".to_string(), alias: None },
    /// #         ops: vec![],
    /// #     },
    /// # };
    /// let translator = SubstraitTranslator::new(&schema_provider);
    /// let plan = translator.translate(&program).expect("Translation failed");
    /// ```
    pub fn translate(&self, program: &Program) -> Result<Plan, TranslateError> {
        // Translate the main pipeline to a relation
        let root_rel = self.translate_pipeline(&program.pipeline)?;

        // Calculate the FINAL output column names based on the pipeline
        let names = self.get_pipeline_output_names(&program.pipeline)?;

        // Wrap in PlanRel
        let plan_rel = substrait::proto::PlanRel {
            rel_type: Some(substrait::proto::plan_rel::RelType::Root(
                substrait::proto::RelRoot {
                    input: Some(root_rel),
                    names, // Output column names
                },
            )),
        };

        // Generate extension URIs and function extensions
        let (extension_uris, extensions) = self.generate_extensions();

        let plan = Plan {
            version: Some(substrait::proto::Version {
                minor_number: 53,
                patch_number: 0,
                ..Default::default()
            }),
            extension_uris,
            extensions,
            relations: vec![plan_rel],
            ..Default::default()
        };

        Ok(plan)
    }

    /// Generate extension URIs and extensions based on registered functions
    fn generate_extensions(&self) -> (Vec<substrait::proto::extensions::SimpleExtensionUri>, Vec<substrait::proto::extensions::SimpleExtensionDeclaration>) {
        let registry = self.function_registry.borrow();
        let functions = registry.get_functions();

        if functions.is_empty() {
            return (vec![], vec![]);
        }

        // Create extension URI for Substrait standard functions
        // Note: functions_arithmetic.yaml includes both arithmetic AND aggregate functions (sum, count, avg, etc.)
        let extension_uri = substrait::proto::extensions::SimpleExtensionUri {
            extension_uri_anchor: 1,
            uri: "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml".to_string(),
        };

        // Create extension function declarations
        let extensions = functions.iter().map(|(function_name, anchor)| {
            substrait::proto::extensions::SimpleExtensionDeclaration {
                mapping_type: Some(substrait::proto::extensions::simple_extension_declaration::MappingType::ExtensionFunction(
                    substrait::proto::extensions::simple_extension_declaration::ExtensionFunction {
                        extension_uri_reference: 1, // Reference to the URI
                        extension_urn_reference: 0, // Deprecated field (0 = not used)
                        function_anchor: *anchor,
                        name: function_name.clone(),
                    }
                )),
            }
        }).collect();

        (vec![extension_uri], extensions)
    }

    /// Get the output column names for a source
    fn get_output_names(&self, source: &Source) -> Result<Vec<String>, TranslateError> {
        match source {
            Source::Table { name, alias: _ } => {
                let schema = self.schema_provider
                    .get_table_schema(name)
                    .map_err(TranslateError::Schema)?;
                Ok(schema.columns.iter().map(|c| c.name.clone()).collect())
            }
            _ => Err(TranslateError::UnsupportedOperator("Only Table sources supported currently".to_string())),
        }
    }

    /// Calculate the FINAL output schema of a pipeline after all operators
    fn get_pipeline_output_names(&self, pipeline: &Pipeline) -> Result<Vec<String>, TranslateError> {
        let mut current_schema = self.get_output_names(&pipeline.source)?;

        // Trace through operators to calculate final schema
        for op in &pipeline.ops {
            current_schema = match op {
                Operator::Select { projections } => {
                    // Select changes the schema to the projected columns
                    let mut result = Vec::new();
                    for (idx, proj) in projections.iter().enumerate() {
                        match proj {
                            Projection::Expr(Expr::Column { col }) => {
                                result.push(col.column.clone());
                            }
                            Projection::Aliased { alias, .. } => {
                                result.push(alias.clone());
                            }
                            Projection::Expr(_) => {
                                // For non-column expressions without alias, generate name
                                result.push(format!("expr_{}", idx));
                            }
                        }
                    }
                    result
                }
                Operator::GroupBy { keys, aggs } => {
                    // GroupBy output: grouping keys + aggregate aliases
                    let mut output = Vec::new();
                    for key in keys {
                        output.push(key.column.clone());
                    }
                    for (alias, _) in aggs {
                        output.push(alias.clone());
                    }
                    output
                }
                Operator::Join { source, .. } => {
                    // Join output: [left_columns..., right_columns...]
                    let right_schema = self.get_output_names(source)?;
                    let mut output = current_schema.clone();
                    output.extend(right_schema);
                    output
                }
                // Most operators preserve the schema
                Operator::Filter { .. } |
                Operator::Sort { .. } |
                Operator::Take { .. } |
                Operator::Distinct => {
                    current_schema // No change
                }
                _ => {
                    return Err(TranslateError::UnsupportedOperator(format!("Output schema calculation not implemented for operator: {:?}", op)));
                }
            };
        }

        Ok(current_schema)
    }

    fn translate_pipeline(&self, pipeline: &Pipeline) -> Result<substrait::proto::Rel, TranslateError> {
        // Check if we have a GroupBy operator that needs projection in Read
        let needs_projection = pipeline.ops.iter().any(|op| matches!(op, Operator::GroupBy { .. }));

        let projection_fields = if needs_projection {
            // Calculate which columns are needed for GroupBy
            self.calculate_groupby_projection(pipeline)?
        } else {
            None
        };

        // Start with the source and get the initial schema
        let mut rel = self.translate_source_with_projection(&pipeline.source, projection_fields.as_ref())?;

        // Get the schema context from the source
        let current_schema = if let Some(ref fields) = projection_fields {
            // If projection is applied, schema is the projected columns
            let full_schema = self.get_output_names(&pipeline.source)?;
            fields.iter().map(|&idx| full_schema[idx].clone()).collect()
        } else {
            self.get_output_names(&pipeline.source)?
        };

        // Apply operators on top of the source relation
        for op in &pipeline.ops {
            rel = self.translate_operator(op, rel, &current_schema)?;
            // TODO: Update current_schema if operator changes column set (e.g., Select)
        }

        Ok(rel)
    }

    fn calculate_groupby_projection(&self, pipeline: &Pipeline) -> Result<Option<Vec<usize>>, TranslateError> {
        // Find GroupBy operator and collect needed columns
        for op in &pipeline.ops {
            if let Operator::GroupBy { keys, aggs } = op {
                let full_schema = self.get_output_names(&pipeline.source)?;
                let mut needed_indices = Vec::new();

                // Add grouping key column indices
                for key in keys {
                    let idx = full_schema.iter().position(|name| name == &key.column)
                        .ok_or_else(|| TranslateError::Translation(format!("Column '{}' not found", key.column)))?;
                    if !needed_indices.contains(&idx) {
                        needed_indices.push(idx);
                    }
                }

                // Add aggregate argument column indices
                for agg_call in aggs.values() {
                    for expr in &agg_call.args {
                        if let Expr::Column { col } = expr {
                            let idx = full_schema.iter().position(|name| name == &col.column)
                                .ok_or_else(|| TranslateError::Translation(format!("Column '{}' not found", col.column)))?;
                            if !needed_indices.contains(&idx) {
                                needed_indices.push(idx);
                            }
                        }
                    }
                }

                return Ok(Some(needed_indices));
            }
        }
        Ok(None)
    }

    fn translate_source_with_projection(&self, source: &Source, projection: Option<&Vec<usize>>) -> Result<substrait::proto::Rel, TranslateError> {
        match source {
            Source::Table { name, alias: _ } => {
                // Look up schema from provider
                let schema = self.schema_provider
                    .get_table_schema(name)
                    .map_err(TranslateError::Schema)?;

                // Build NamedStruct for base_schema
                let named_struct = substrait::proto::NamedStruct {
                    names: schema.columns.iter().map(|c| c.name.clone()).collect(),
                    r#struct: Some(substrait::proto::r#type::Struct {
                        types: schema.columns.iter().map(|c| {
                            self.map_type(&c.data_type, c.nullable)
                        }).collect(),
                        type_variation_reference: 0,
                        nullability: substrait::proto::r#type::Nullability::Required as i32,
                    }),
                };

                // Create projection if needed
                let projection_expr = projection.map(|fields| {
                    substrait::proto::expression::MaskExpression {
                        select: Some(substrait::proto::expression::mask_expression::StructSelect {
                            struct_items: fields.iter().map(|&idx| {
                                substrait::proto::expression::mask_expression::StructItem {
                                    field: idx as i32,
                                    child: None,
                                }
                            }).collect(),
                        }),
                        maintain_singular_struct: true,
                    }
                });

                // Create ReadRel with NamedTable and optional projection
                let read_rel = substrait::proto::ReadRel {
                    common: None,
                    base_schema: Some(named_struct),
                    filter: None,
                    best_effort_filter: None,
                    projection: projection_expr,
                    advanced_extension: None,
                    read_type: Some(substrait::proto::read_rel::ReadType::NamedTable(
                        substrait::proto::read_rel::NamedTable {
                            names: vec![name.clone()],
                            advanced_extension: None,
                        },
                    )),
                };

                Ok(substrait::proto::Rel {
                    rel_type: Some(substrait::proto::rel::RelType::Read(Box::new(read_rel))),
                })
            }
            _ => Err(TranslateError::UnsupportedOperator("Only Table sources supported currently".to_string())),
        }
    }

    fn translate_operator(&self, op: &Operator, input: substrait::proto::Rel, schema: &[String]) -> Result<substrait::proto::Rel, TranslateError> {
        match op {
            Operator::Filter { condition } => self.translate_filter(input, condition, schema),
            Operator::Select { projections } => self.translate_select(input, projections, schema),
            Operator::Sort { keys } => self.translate_sort(input, keys, schema),
            Operator::Take { limit } => self.translate_take(input, *limit),
            Operator::Distinct => self.translate_distinct(input, schema),
            Operator::GroupBy { keys, aggs } => self.translate_groupby(input, keys, aggs, schema),
            Operator::Join { source, on, join_type } => self.translate_join(input, source, on, join_type, schema),
            _ => Err(TranslateError::UnsupportedOperator(format!("Operator {:?} not yet supported", op))),
        }
    }

    fn translate_filter(&self, input: substrait::proto::Rel, condition: &Expr, schema: &[String]) -> Result<substrait::proto::Rel, TranslateError> {
        // Convert the condition expression to a Substrait expression
        let substrait_condition = self.translate_expr(condition, schema)?;

        // Create FilterRel
        let filter_rel = substrait::proto::FilterRel {
            common: None,
            input: Some(Box::new(input)),
            condition: Some(Box::new(substrait_condition)),
            advanced_extension: None,
        };

        // Wrap in Rel
        Ok(substrait::proto::Rel {
            rel_type: Some(substrait::proto::rel::RelType::Filter(Box::new(filter_rel))),
        })
    }

    fn translate_select(&self, input: substrait::proto::Rel, projections: &[Projection], schema: &[String]) -> Result<substrait::proto::Rel, TranslateError> {
        // Convert each projection to a Substrait expression
        let expressions: Result<Vec<_>, _> = projections.iter().map(|proj| {
            match proj {
                Projection::Expr(expr) => self.translate_expr(expr, schema),
                Projection::Aliased { expr, alias: _ } => {
                    // For now, just translate the expression
                    // Aliases are handled at the relation level (output names)
                    self.translate_expr(expr, schema)
                }
            }
        }).collect();

        let expressions = expressions?;

        // Create ProjectRel
        let project_rel = substrait::proto::ProjectRel {
            common: None,
            input: Some(Box::new(input)),
            expressions,
            advanced_extension: None,
        };

        // Wrap in Rel
        Ok(substrait::proto::Rel {
            rel_type: Some(substrait::proto::rel::RelType::Project(Box::new(project_rel))),
        })
    }

    fn translate_sort(&self, input: substrait::proto::Rel, keys: &[SortKey], schema: &[String]) -> Result<substrait::proto::Rel, TranslateError> {
        // Convert each sort key to a Substrait SortField
        let sorts: Result<Vec<_>, _> = keys.iter().map(|key| {
            let expr = self.translate_expr(&key.expr, schema)?;

            // Map MLQL desc flag to Substrait SortDirection
            // Protobuf enum values: ASC_NULLS_FIRST=1, ASC_NULLS_LAST=2, DESC_NULLS_FIRST=3, DESC_NULLS_LAST=4
            let direction = if key.desc {
                4  // SORT_DIRECTION_DESC_NULLS_LAST
            } else {
                1  // SORT_DIRECTION_ASC_NULLS_FIRST
            };

            Ok(substrait::proto::SortField {
                expr: Some(expr),
                sort_kind: Some(substrait::proto::sort_field::SortKind::Direction(direction)),
            })
        }).collect();

        let sorts = sorts?;

        // Create SortRel
        let sort_rel = substrait::proto::SortRel {
            common: None,
            input: Some(Box::new(input)),
            sorts,
            advanced_extension: None,
        };

        // Wrap in Rel
        Ok(substrait::proto::Rel {
            rel_type: Some(substrait::proto::rel::RelType::Sort(Box::new(sort_rel))),
        })
    }

    fn translate_take(&self, input: substrait::proto::Rel, limit: i64) -> Result<substrait::proto::Rel, TranslateError> {
        // Create FetchRel (Substrait's LIMIT operator)
        // NOTE: We use the DEPRECATED oneof variants because DuckDB v1.3 substrait extension
        // calls the deprecated .offset() and .count() accessor methods.
        // See: duckdb-substrait-upgrade/src/from_substrait.cpp:539-540
        let fetch_rel = substrait::proto::FetchRel {
            common: None,
            input: Some(Box::new(input)),
            offset_mode: Some(substrait::proto::fetch_rel::OffsetMode::Offset(0)),  // Use deprecated variant
            count_mode: Some(substrait::proto::fetch_rel::CountMode::Count(limit)),  // Use deprecated variant
            advanced_extension: None,
        };

        // Wrap in Rel
        Ok(substrait::proto::Rel {
            rel_type: Some(substrait::proto::rel::RelType::Fetch(Box::new(fetch_rel))),
        })
    }

    fn translate_distinct(&self, input: substrait::proto::Rel, schema: &[String]) -> Result<substrait::proto::Rel, TranslateError> {
        // DISTINCT is implemented as an AggregateRel with grouping on all columns and no measures
        // This is the standard Substrait pattern for deduplication
        //
        // NOTE: DuckDB v1.4.0 substrait extension uses the DEPRECATED `grouping_expressions` field
        // inside Grouping (see duckdb-substrait-upgrade/src/from_substrait.cpp:664), not the new
        // `expression_references` approach. We must use the deprecated API for compatibility.

        // Create grouping expressions for all columns
        let grouping_expressions: Result<Vec<_>, _> = schema.iter().enumerate().map(|(idx, _name)| {
            // Create a field reference for each column
            // Match DuckDB's format: include rootReference (empty RootReference message)
            Ok(substrait::proto::Expression {
                rex_type: Some(substrait::proto::expression::RexType::Selection(Box::new(
                    substrait::proto::expression::FieldReference {
                        reference_type: Some(substrait::proto::expression::field_reference::ReferenceType::DirectReference(
                            substrait::proto::expression::ReferenceSegment {
                                reference_type: Some(substrait::proto::expression::reference_segment::ReferenceType::StructField(Box::new(
                                    substrait::proto::expression::reference_segment::StructField {
                                        field: idx as i32,
                                        child: None,
                                    }
                                ))),
                            }
                        )),
                        root_type: Some(substrait::proto::expression::field_reference::RootType::RootReference(
                            substrait::proto::expression::field_reference::RootReference {}
                        )),
                    }
                ))),
            })
        }).collect();

        let grouping_expressions = grouping_expressions?;

        // Create a single grouping with all column expressions (using deprecated field for DuckDB compatibility)
        #[allow(deprecated)]
        let grouping = substrait::proto::aggregate_rel::Grouping {
            grouping_expressions: grouping_expressions.clone(), // Use deprecated field (DuckDB reads this)
            expression_references: vec![], // Leave empty (DuckDB ignores this)
        };

        // Create AggregateRel with grouping but no measures (aggregates)
        let aggregate_rel = substrait::proto::AggregateRel {
            common: None,
            input: Some(Box::new(input)),
            groupings: vec![grouping],
            measures: vec![], // Empty measures = DISTINCT
            grouping_expressions: vec![], // Empty for deprecated approach (DuckDB uses grouping_expressions from Grouping)
            advanced_extension: None,
        };

        // Wrap in Rel
        Ok(substrait::proto::Rel {
            rel_type: Some(substrait::proto::rel::RelType::Aggregate(Box::new(aggregate_rel))),
        })
    }

    fn translate_join(&self, left_input: substrait::proto::Rel, right_source: &Source, condition: &Expr, join_type: &Option<JoinType>, left_schema: &[String]) -> Result<substrait::proto::Rel, TranslateError> {
        // Translate the right source (typically a table)
        let right_rel = self.translate_source_with_projection(right_source, None)?;
        let right_schema = self.get_output_names(right_source)?;

        // Combined schema: [left_cols..., right_cols...]
        let mut combined_schema = left_schema.to_vec();
        combined_schema.extend(right_schema.iter().cloned());

        // Translate the join condition with combined schema
        let join_expr = self.translate_expr(condition, &combined_schema)?;

        // Map MLQL JoinType to Substrait JoinType enum value
        let substrait_join_type = match join_type {
            None | Some(JoinType::Inner) => 1,  // JOIN_TYPE_INNER
            Some(JoinType::Left) => 3,           // JOIN_TYPE_LEFT
            Some(JoinType::Right) => 4,          // JOIN_TYPE_RIGHT
            Some(JoinType::Full) => 2,           // JOIN_TYPE_OUTER (Full Outer)
            Some(JoinType::Semi) => 5,           // JOIN_TYPE_LEFT_SEMI
            Some(JoinType::Anti) => 6,           // JOIN_TYPE_LEFT_ANTI
            Some(JoinType::Cross) => {
                // Cross join is a special case - no condition
                return Err(TranslateError::UnsupportedOperator("Cross join not yet supported".to_string()));
            }
        };

        // Create JoinRel
        let join_rel = substrait::proto::JoinRel {
            common: None,
            left: Some(Box::new(left_input)),
            right: Some(Box::new(right_rel)),
            expression: Some(Box::new(join_expr)),
            post_join_filter: None,
            r#type: substrait_join_type,
            advanced_extension: None,
        };

        // Wrap in Rel
        Ok(substrait::proto::Rel {
            rel_type: Some(substrait::proto::rel::RelType::Join(Box::new(join_rel))),
        })
    }

    fn translate_groupby(&self, input: substrait::proto::Rel, keys: &[ColumnRef], aggs: &HashMap<String, AggCall>, schema: &[String]) -> Result<substrait::proto::Rel, TranslateError> {
        // GroupBy translates to AggregateRel with:
        // - grouping_expressions: the grouping keys
        // - measures: the aggregate functions
        //
        // IMPORTANT: Since we add projection to ReadRel, the schema passed here is the PROJECTED schema.
        // We use rootReference to refer back to the Read's projected output.
        //
        // NOTE: Like Distinct, we use the deprecated `grouping_expressions` field for DuckDB compatibility

        // Create grouping expressions from the keys with rootReference
        let grouping_expressions: Result<Vec<_>, _> = keys.iter().map(|key| {
            // Find the column index in the projected schema
            let idx = schema.iter().position(|name| name == &key.column)
                .ok_or_else(|| TranslateError::Translation(format!("Column '{}' not found in schema", key.column)))?;

            // Create field reference WITH rootReference (DuckDB format)
            Ok(substrait::proto::Expression {
                rex_type: Some(substrait::proto::expression::RexType::Selection(Box::new(
                    substrait::proto::expression::FieldReference {
                        reference_type: Some(substrait::proto::expression::field_reference::ReferenceType::DirectReference(
                            substrait::proto::expression::ReferenceSegment {
                                reference_type: Some(substrait::proto::expression::reference_segment::ReferenceType::StructField(Box::new(
                                    substrait::proto::expression::reference_segment::StructField {
                                        field: idx as i32,
                                        child: None,
                                    }
                                ))),
                            }
                        )),
                        root_type: Some(substrait::proto::expression::field_reference::RootType::RootReference(
                            substrait::proto::expression::field_reference::RootReference {}
                        )),
                    }
                ))),
            })
        }).collect();

        let grouping_expressions = grouping_expressions?;

        // Create a single grouping (using deprecated field for DuckDB compatibility)
        #[allow(deprecated)]
        let grouping = substrait::proto::aggregate_rel::Grouping {
            grouping_expressions: grouping_expressions.clone(),
            expression_references: vec![],
        };

        // Create measures (aggregate functions) with rootReference
        let measures: Result<Vec<_>, _> = aggs.iter().map(|(name, agg_call)| {
            self.translate_aggregate_with_root(agg_call, schema, name)
        }).collect();

        let measures = measures?;

        // Create AggregateRel
        let aggregate_rel = substrait::proto::AggregateRel {
            common: None,
            input: Some(Box::new(input)),
            groupings: vec![grouping],
            measures,
            grouping_expressions: vec![], // Empty for deprecated approach
            advanced_extension: None,
        };

        // Wrap in Rel
        Ok(substrait::proto::Rel {
            rel_type: Some(substrait::proto::rel::RelType::Aggregate(Box::new(aggregate_rel))),
        })
    }

    fn translate_aggregate_with_root(&self, agg_call: &AggCall, schema: &[String], _name: &str) -> Result<substrait::proto::aggregate_rel::Measure, TranslateError> {
        // Translate aggregate function arguments with rootReference
        let arguments: Result<Vec<_>, _> = agg_call.args.iter().map(|expr| {
            let expr_result = match expr {
                Expr::Column { col } => self.translate_column_ref_with_root(col, schema, true)?,
                _ => self.translate_expr(expr, schema)?,
            };
            Ok(substrait::proto::FunctionArgument {
                arg_type: Some(substrait::proto::function_argument::ArgType::Value(expr_result)),
            })
        }).collect();

        let arguments = arguments?;

        // Register the aggregate function and get its anchor
        let function_sig = format!("{}:i32", agg_call.func);
        let function_anchor = self.function_registry.borrow_mut().register(&function_sig);

        // Create output type for aggregate function (i64 for sum)
        let output_type = Some(substrait::proto::Type {
            kind: Some(substrait::proto::r#type::Kind::I64(
                substrait::proto::r#type::I64 {
                    type_variation_reference: 0,
                    nullability: substrait::proto::r#type::Nullability::Nullable as i32,
                }
            )),
        });

        // Create AggregateFunction
        let agg_function = substrait::proto::AggregateFunction {
            function_reference: function_anchor,
            arguments,
            sorts: vec![],
            invocation: 0,
            phase: 0,
            output_type,
            options: vec![],
            #[allow(deprecated)]
            args: vec![],
        };

        Ok(substrait::proto::aggregate_rel::Measure {
            measure: Some(agg_function),
            filter: None,
        })
    }

    fn translate_aggregate(&self, agg_call: &AggCall, schema: &[String], _name: &str) -> Result<substrait::proto::aggregate_rel::Measure, TranslateError> {
        // Translate aggregate function arguments
        let arguments: Result<Vec<_>, _> = agg_call.args.iter().map(|expr| {
            let expr_result = self.translate_expr(expr, schema)?;
            Ok(substrait::proto::FunctionArgument {
                arg_type: Some(substrait::proto::function_argument::ArgType::Value(expr_result)),
            })
        }).collect();

        let arguments = arguments?;

        // Register the aggregate function and get its anchor
        // Use proper type signature (i32 for INTEGER columns)
        let function_sig = format!("{}:i32", agg_call.func);
        let function_anchor = self.function_registry.borrow_mut().register(&function_sig);

        // Create output type for aggregate function (i64 for sum)
        let output_type = Some(substrait::proto::Type {
            kind: Some(substrait::proto::r#type::Kind::I64(
                substrait::proto::r#type::I64 {
                    type_variation_reference: 0,
                    nullability: substrait::proto::r#type::Nullability::Nullable as i32,
                }
            )),
        });

        // Create AggregateFunction
        let agg_function = substrait::proto::AggregateFunction {
            function_reference: function_anchor,
            arguments,
            sorts: vec![],
            invocation: 0, // AGGREGATION_INVOCATION_UNSPECIFIED
            phase: 0, // AGGREGATION_PHASE_UNSPECIFIED
            output_type,
            options: vec![],
            #[allow(deprecated)]
            args: vec![], // Deprecated field
        };

        // Create Measure
        Ok(substrait::proto::aggregate_rel::Measure {
            measure: Some(agg_function),
            filter: None,
        })
    }

    fn translate_expr(&self, expr: &Expr, schema: &[String]) -> Result<substrait::proto::Expression, TranslateError> {
        match expr {
            Expr::Literal { value } => self.translate_literal(value),
            Expr::Column { col } => self.translate_column_ref(col, schema),
            Expr::BinaryOp { op, left, right } => self.translate_binary_op(op, left, right, schema),
            Expr::UnaryOp { op, expr } => self.translate_unary_op(op, expr, schema),
            _ => Err(TranslateError::UnsupportedOperator(format!("Expression {:?} not yet supported", expr))),
        }
    }

    fn translate_literal(&self, value: &Value) -> Result<substrait::proto::Expression, TranslateError> {
        let literal = match value {
            Value::Null => substrait::proto::expression::Literal {
                nullable: true,
                type_variation_reference: 0,
                literal_type: Some(substrait::proto::expression::literal::LiteralType::Null(
                    substrait::proto::Type { kind: None }
                )),
            },
            Value::Bool(b) => substrait::proto::expression::Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(substrait::proto::expression::literal::LiteralType::Boolean(*b)),
            },
            Value::Int(i) => substrait::proto::expression::Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(substrait::proto::expression::literal::LiteralType::I64(*i)),
            },
            Value::Float(f) => substrait::proto::expression::Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(substrait::proto::expression::literal::LiteralType::Fp64(*f)),
            },
            Value::String(s) => substrait::proto::expression::Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(substrait::proto::expression::literal::LiteralType::String(s.clone())),
            },
            _ => return Err(TranslateError::UnsupportedOperator(format!("Literal value {:?} not yet supported", value))),
        };

        Ok(substrait::proto::Expression {
            rex_type: Some(substrait::proto::expression::RexType::Literal(literal)),
        })
    }

    fn translate_column_ref(&self, col: &ColumnRef, schema: &[String]) -> Result<substrait::proto::Expression, TranslateError> {
        self.translate_column_ref_with_root(col, schema, false)
    }

    fn translate_column_ref_with_root(&self, col: &ColumnRef, schema: &[String], use_root_reference: bool) -> Result<substrait::proto::Expression, TranslateError> {
        // Resolve column name to field index
        let column_name = &col.column;

        // Find the column index in the schema
        let field_index = schema.iter()
            .position(|name| name == column_name)
            .ok_or_else(|| TranslateError::Translation(
                format!("Column '{}' not found in schema. Available columns: {:?}", column_name, schema)
            ))?;


        // Create a FieldReference (direct field reference by index)
        let field_ref = substrait::proto::expression::FieldReference {
            reference_type: Some(substrait::proto::expression::field_reference::ReferenceType::DirectReference(
                substrait::proto::expression::ReferenceSegment {
                    reference_type: Some(substrait::proto::expression::reference_segment::ReferenceType::StructField(
                        Box::new(substrait::proto::expression::reference_segment::StructField {
                            field: field_index as i32,
                            child: None,
                        })
                    )),
                }
            )),
            root_type: if use_root_reference {
                Some(substrait::proto::expression::field_reference::RootType::RootReference(
                    substrait::proto::expression::field_reference::RootReference {}
                ))
            } else {
                None
            },
        };

        Ok(substrait::proto::Expression {
            rex_type: Some(substrait::proto::expression::RexType::Selection(Box::new(field_ref))),
        })
    }

    fn translate_binary_op(&self, op: &BinOp, left: &Expr, right: &Expr, schema: &[String]) -> Result<substrait::proto::Expression, TranslateError> {
        let left_expr = Box::new(self.translate_expr(left, schema)?);
        let right_expr = Box::new(self.translate_expr(right, schema)?);

        // Map MLQL binary operator to Substrait function base name
        let function_base_name = match op {
            BinOp::Eq => "equal",
            BinOp::Ne => "not_equal",
            BinOp::Lt => "lt",
            BinOp::Le => "lte",
            BinOp::Gt => "gt",
            BinOp::Ge => "gte",
            BinOp::And => "and",
            BinOp::Or => "or",
            BinOp::Add => "add",
            BinOp::Sub => "subtract",
            BinOp::Mul => "multiply",
            BinOp::Div => "divide",
            BinOp::Like => "like",
            BinOp::ILike => "ilike",
            _ => return Err(TranslateError::UnsupportedOperator(format!("Binary operator {:?} not yet supported", op))),
        };

        // For now, assume i32 types for comparisons (TODO: infer actual types)
        // DuckDB function signature format: "function_name:arg1_type_arg2_type"
        let function_signature = format!("{}:i32_i32", function_base_name);

        // Register the function and get its anchor
        let function_anchor = self.function_registry.borrow_mut().register(&function_signature);

        // Create scalar function call
        let scalar_function = substrait::proto::expression::ScalarFunction {
            function_reference: function_anchor,
            arguments: vec![
                substrait::proto::FunctionArgument {
                    arg_type: Some(substrait::proto::function_argument::ArgType::Value(*left_expr)),
                },
                substrait::proto::FunctionArgument {
                    arg_type: Some(substrait::proto::function_argument::ArgType::Value(*right_expr)),
                },
            ],
            output_type: None, // Type inference
            options: vec![],
            args: vec![], // Deprecated field
        };

        Ok(substrait::proto::Expression {
            rex_type: Some(substrait::proto::expression::RexType::ScalarFunction(scalar_function)),
        })
    }

    fn translate_unary_op(&self, op: &UnOp, expr: &Expr, schema: &[String]) -> Result<substrait::proto::Expression, TranslateError> {
        let inner_expr = Box::new(self.translate_expr(expr, schema)?);

        let function_base_name = match op {
            UnOp::Not => "not",
            UnOp::Neg => "negate",
        };

        // For now, assume bool type for not, i32 for negate
        let function_signature = match op {
            UnOp::Not => format!("{}:bool", function_base_name),
            UnOp::Neg => format!("{}:i32", function_base_name),
        };

        // Register the function and get its anchor
        let function_anchor = self.function_registry.borrow_mut().register(&function_signature);

        // Create scalar function call
        let scalar_function = substrait::proto::expression::ScalarFunction {
            function_reference: function_anchor,
            arguments: vec![
                substrait::proto::FunctionArgument {
                    arg_type: Some(substrait::proto::function_argument::ArgType::Value(*inner_expr)),
                },
            ],
            output_type: None,
            options: vec![],
            args: vec![],
        };

        Ok(substrait::proto::Expression {
            rex_type: Some(substrait::proto::expression::RexType::ScalarFunction(scalar_function)),
        })
    }

    fn translate_source(&self, source: &Source) -> Result<substrait::proto::Rel, TranslateError> {
        match source {
            Source::Table { name, alias: _ } => {
                // Look up schema from provider
                let schema = self.schema_provider
                    .get_table_schema(name)
                    .map_err(TranslateError::Schema)?;

                // Build NamedStruct for base_schema
                let named_struct = substrait::proto::NamedStruct {
                    names: schema.columns.iter().map(|c| c.name.clone()).collect(),
                    r#struct: Some(substrait::proto::r#type::Struct {
                        types: schema.columns.iter().map(|c| {
                            // Map column type to Substrait type
                            self.map_type(&c.data_type, c.nullable)
                        }).collect(),
                        type_variation_reference: 0,
                        nullability: substrait::proto::r#type::Nullability::Unspecified as i32,
                    }),
                };

                // Create ReadRel with NamedTable
                let read_rel = substrait::proto::ReadRel {
                    common: None, // RelCommon - optional
                    base_schema: Some(named_struct),
                    filter: None,
                    best_effort_filter: None,
                    projection: None,
                    advanced_extension: None,
                    read_type: Some(substrait::proto::read_rel::ReadType::NamedTable(
                        substrait::proto::read_rel::NamedTable {
                            names: vec![name.clone()],
                            advanced_extension: None,
                        },
                    )),
                };

                Ok(substrait::proto::Rel {
                    rel_type: Some(substrait::proto::rel::RelType::Read(Box::new(read_rel))),
                })
            }
            _ => Err(TranslateError::UnsupportedOperator("Only Table sources supported currently".to_string())),
        }
    }

    /// Map MLQL type string to Substrait Type
    fn map_type(&self, type_str: &str, nullable: bool) -> substrait::proto::Type {
        let nullability = if nullable {
            substrait::proto::r#type::Nullability::Nullable as i32
        } else {
            substrait::proto::r#type::Nullability::Required as i32
        };

        let kind = match type_str.to_uppercase().as_str() {
            "INTEGER" | "INT" | "INT32" => substrait::proto::r#type::Kind::I32(
                substrait::proto::r#type::I32 {
                    type_variation_reference: 0,
                    nullability,
                },
            ),
            "BIGINT" | "INT64" => substrait::proto::r#type::Kind::I64(
                substrait::proto::r#type::I64 {
                    type_variation_reference: 0,
                    nullability,
                },
            ),
            "VARCHAR" | "STRING" | "TEXT" => substrait::proto::r#type::Kind::String(
                substrait::proto::r#type::String {
                    type_variation_reference: 0,
                    nullability,
                },
            ),
            "DOUBLE" | "FLOAT64" => substrait::proto::r#type::Kind::Fp64(
                substrait::proto::r#type::Fp64 {
                    type_variation_reference: 0,
                    nullability,
                },
            ),
            "FLOAT" | "FLOAT32" => substrait::proto::r#type::Kind::Fp32(
                substrait::proto::r#type::Fp32 {
                    type_variation_reference: 0,
                    nullability,
                },
            ),
            _ => {
                // Default to string for unknown types
                substrait::proto::r#type::Kind::String(
                    substrait::proto::r#type::String {
                        type_variation_reference: 0,
                        nullability,
                    },
                )
            }
        };

        substrait::proto::Type {
            kind: Some(kind),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::substrait::schema::{MockSchemaProvider, TableSchema, ColumnInfo};
    use crate::{Program, Pipeline, Source};

    #[test]
    fn test_simple_table_scan() {
        // Setup: Create a mock schema provider with a simple table
        let mut schema_provider = MockSchemaProvider::new();
        schema_provider.add_table(TableSchema {
            name: "users".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                },
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: "VARCHAR".to_string(),
                    nullable: true,
                },
            ],
        });

        // Create IR Program: from users
        let program = Program {
            pragma: None,
            lets: vec![],
            pipeline: Pipeline {
                source: Source::Table {
                    name: "users".to_string(),
                    alias: None,
                },
                ops: vec![],
            },
        };

        // Translate to Substrait
        let translator = SubstraitTranslator::new(&schema_provider);
        let result = translator.translate(&program);

        // Verify it succeeds
        assert!(result.is_ok(), "Translation failed: {:?}", result.err());

        let plan = result.unwrap();

        // Basic checks
        assert!(plan.version.is_some(), "Plan should have version");
        assert_eq!(plan.relations.len(), 1, "Plan should have exactly one relation");

        println!("✅ Successfully translated simple table scan to Substrait Plan");
        println!("   Version: {:?}", plan.version);
        println!("   Relations count: {}", plan.relations.len());
    }

    #[test]
    fn test_substrait_plan_generation() {
        use prost::Message;

        // Setup schema provider
        let mut schema_provider = MockSchemaProvider::new();
        schema_provider.add_table(TableSchema {
            name: "test_table".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                },
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: "VARCHAR".to_string(),
                    nullable: true,
                },
            ],
        });

        // Create IR Program: from test_table
        let program = Program {
            pragma: None,
            lets: vec![],
            pipeline: Pipeline {
                source: Source::Table {
                    name: "test_table".to_string(),
                    alias: None,
                },
                ops: vec![],
            },
        };

        // Translate to Substrait
        let translator = SubstraitTranslator::new(&schema_provider);
        let plan = translator.translate(&program).expect("Translation should succeed");

        // Verify plan structure
        assert!(plan.version.is_some(), "Plan should have version");
        assert_eq!(plan.version.as_ref().unwrap().minor_number, 53);
        assert_eq!(plan.relations.len(), 1, "Plan should have exactly one relation");

        // Verify it's a Root relation
        let plan_rel = plan.relations.first().unwrap();
        assert!(plan_rel.rel_type.is_some(), "PlanRel should have rel_type");

        // Extract root
        if let Some(substrait::proto::plan_rel::RelType::Root(root)) = &plan_rel.rel_type {
            assert!(root.input.is_some(), "Root should have input");
            let input = root.input.as_ref().unwrap();

            // Verify it's a ReadRel
            if let Some(substrait::proto::rel::RelType::Read(read_rel)) = &input.rel_type {
                // Verify schema
                assert!(read_rel.base_schema.is_some(), "ReadRel should have schema");
                let schema = read_rel.base_schema.as_ref().unwrap();
                assert_eq!(schema.names.len(), 2, "Schema should have 2 columns");
                assert_eq!(schema.names[0], "id");
                assert_eq!(schema.names[1], "name");

                // Verify it's a NamedTable
                if let Some(substrait::proto::read_rel::ReadType::NamedTable(named_table)) = &read_rel.read_type {
                    assert_eq!(named_table.names, vec!["test_table"]);
                } else {
                    panic!("ReadRel should have NamedTable");
                }
            } else {
                panic!("Input should be ReadRel");
            }
        } else {
            panic!("PlanRel should be Root");
        }

        // Test serialization
        let plan_bytes = plan.encode_to_vec();
        assert!(plan_bytes.len() > 0, "Plan should serialize to protobuf");

        // Debug: serialize to JSON to inspect structure
        let plan_json = serde_json::to_string_pretty(&plan).expect("Failed to serialize to JSON");
        println!("Generated Substrait Plan JSON:");
        println!("{}", plan_json);

        println!("✅ Substrait plan generation test passed");
        println!("   Generated {} bytes", plan_bytes.len());
    }

    #[test]
    fn test_filter_with_comparison() {
        use prost::Message;

        // Setup schema provider with users table
        let mut schema_provider = MockSchemaProvider::new();
        schema_provider.add_table(TableSchema {
            name: "users".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                },
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: "VARCHAR".to_string(),
                    nullable: true,
                },
                ColumnInfo {
                    name: "age".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                },
            ],
        });

        // Create IR Program: from users | filter age > 18
        let program = Program {
            pragma: None,
            lets: vec![],
            pipeline: Pipeline {
                source: Source::Table {
                    name: "users".to_string(),
                    alias: None,
                },
                ops: vec![
                    Operator::Filter {
                        condition: Expr::BinaryOp {
                            op: BinOp::Gt,
                            left: Box::new(Expr::Column {
                                col: ColumnRef {
                                    table: None,
                                    column: "age".to_string(),
                                },
                            }),
                            right: Box::new(Expr::Literal {
                                value: Value::Int(18),
                            }),
                        },
                    },
                ],
            },
        };

        // Translate to Substrait
        let translator = SubstraitTranslator::new(&schema_provider);
        let result = translator.translate(&program);

        // Verify it succeeds
        assert!(result.is_ok(), "Translation failed: {:?}", result.err());

        let plan = result.unwrap();

        // Verify plan structure
        assert!(plan.version.is_some(), "Plan should have version");
        assert_eq!(plan.relations.len(), 1, "Plan should have exactly one relation");

        // Serialize to protobuf to ensure it's valid
        let plan_bytes = plan.encode_to_vec();
        assert!(plan_bytes.len() > 0, "Plan should serialize to protobuf");

        // Debug: serialize to JSON
        let plan_json = serde_json::to_string_pretty(&plan).expect("Failed to serialize to JSON");
        println!("✅ Filter test passed - Generated Substrait Plan:");
        println!("{}", plan_json);
        println!("   Plan size: {} bytes", plan_bytes.len());
    }

    #[test]
    fn test_select_specific_columns() {
        use prost::Message;

        // Setup schema provider
        let mut schema_provider = MockSchemaProvider::new();
        schema_provider.add_table(TableSchema {
            name: "users".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                },
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: "VARCHAR".to_string(),
                    nullable: true,
                },
                ColumnInfo {
                    name: "age".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                },
            ],
        });

        // Create IR Program: from users | select [name, age]
        let program = Program {
            pragma: None,
            lets: vec![],
            pipeline: Pipeline {
                source: Source::Table {
                    name: "users".to_string(),
                    alias: None,
                },
                ops: vec![
                    Operator::Select {
                        projections: vec![
                            Projection::Expr(Expr::Column {
                                col: ColumnRef {
                                    table: None,
                                    column: "name".to_string(),
                                },
                            }),
                            Projection::Expr(Expr::Column {
                                col: ColumnRef {
                                    table: None,
                                    column: "age".to_string(),
                                },
                            }),
                        ],
                    },
                ],
            },
        };

        // Translate to Substrait
        let translator = SubstraitTranslator::new(&schema_provider);
        let result = translator.translate(&program);

        // Verify it succeeds
        assert!(result.is_ok(), "Translation failed: {:?}", result.err());

        let plan = result.unwrap();

        // Verify plan structure
        assert!(plan.version.is_some(), "Plan should have version");
        assert_eq!(plan.relations.len(), 1, "Plan should have exactly one relation");

        // Serialize to protobuf
        let plan_bytes = plan.encode_to_vec();
        assert!(plan_bytes.len() > 0, "Plan should serialize to protobuf");

        // Debug: serialize to JSON
        let plan_json = serde_json::to_string_pretty(&plan).expect("Failed to serialize to JSON");
        println!("✅ Select test passed - Generated Substrait Plan:");
        println!("{}", plan_json);
        println!("   Plan size: {} bytes", plan_bytes.len());
    }

    #[test]
    fn test_sort_with_multiple_keys() {
        use prost::Message;

        // Setup schema provider
        let mut schema_provider = MockSchemaProvider::new();
        schema_provider.add_table(TableSchema {
            name: "users".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                },
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: "VARCHAR".to_string(),
                    nullable: true,
                },
                ColumnInfo {
                    name: "age".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                },
            ],
        });

        // Create IR Program: from users | sort -age, +name
        // This means: sort by age descending, then by name ascending
        let program = Program {
            pragma: None,
            lets: vec![],
            pipeline: Pipeline {
                source: Source::Table {
                    name: "users".to_string(),
                    alias: None,
                },
                ops: vec![
                    Operator::Sort {
                        keys: vec![
                            SortKey {
                                expr: Expr::Column {
                                    col: ColumnRef {
                                        table: None,
                                        column: "age".to_string(),
                                    },
                                },
                                desc: true,  // -age means descending
                            },
                            SortKey {
                                expr: Expr::Column {
                                    col: ColumnRef {
                                        table: None,
                                        column: "name".to_string(),
                                    },
                                },
                                desc: false,  // +name means ascending
                            },
                        ],
                    },
                ],
            },
        };

        // Translate to Substrait
        let translator = SubstraitTranslator::new(&schema_provider);
        let result = translator.translate(&program);

        // Verify it succeeds
        assert!(result.is_ok(), "Translation failed: {:?}", result.err());

        let plan = result.unwrap();

        // Verify plan structure
        assert!(plan.version.is_some(), "Plan should have version");
        assert_eq!(plan.relations.len(), 1, "Plan should have exactly one relation");

        // Serialize to protobuf
        let plan_bytes = plan.encode_to_vec();
        assert!(plan_bytes.len() > 0, "Plan should serialize to protobuf");

        // Debug: serialize to JSON
        let plan_json = serde_json::to_string_pretty(&plan).expect("Failed to serialize to JSON");
        println!("✅ Sort test passed - Generated Substrait Plan:");
        println!("{}", plan_json);
        println!("   Plan size: {} bytes", plan_bytes.len());
    }

    #[test]
    fn test_take_operator() {
        use prost::Message;

        // Setup schema provider with users table
        let mut schema_provider = MockSchemaProvider::new();
        schema_provider.add_table(TableSchema {
            name: "users".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                },
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: "VARCHAR".to_string(),
                    nullable: true,
                },
                ColumnInfo {
                    name: "age".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                },
            ],
        });

        // Create IR Program: from users | take 10
        let program = Program {
            pragma: None,
            lets: vec![],
            pipeline: Pipeline {
                source: Source::Table {
                    name: "users".to_string(),
                    alias: None,
                },
                ops: vec![
                    Operator::Take {
                        limit: 10,
                    },
                ],
            },
        };

        // Translate to Substrait
        let translator = SubstraitTranslator::new(&schema_provider);
        let result = translator.translate(&program);

        // Verify it succeeds
        assert!(result.is_ok(), "Translation failed: {:?}", result.err());

        let plan = result.unwrap();

        // Verify plan structure
        assert!(plan.version.is_some(), "Plan should have version");
        assert_eq!(plan.relations.len(), 1, "Plan should have exactly one relation");

        // Serialize to protobuf
        let plan_bytes = plan.encode_to_vec();
        assert!(plan_bytes.len() > 0, "Plan should serialize to protobuf");

        // Debug: serialize to JSON
        let plan_json = serde_json::to_string_pretty(&plan).expect("Failed to serialize to JSON");
        println!("✅ Take test passed - Generated Substrait Plan:");
        println!("{}", plan_json);
        println!("   Plan size: {} bytes", plan_bytes.len());
    }
}

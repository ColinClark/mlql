//! Core Substrait translator

use crate::{Program, Pipeline, Source, Operator, Expr, Value, BinOp, UnOp, ColumnRef, Projection, SortKey};
use super::schema::SchemaProvider;
use substrait::proto::Plan;

#[derive(Debug, thiserror::Error)]
pub enum TranslateError {
    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Unsupported operator: {0}")]
    UnsupportedOperator(String),

    #[error("Translation error: {0}")]
    Translation(String),
}

/// Translator for MLQL IR → Substrait
pub struct SubstraitTranslator<'a> {
    schema_provider: &'a dyn SchemaProvider,
}

impl<'a> SubstraitTranslator<'a> {
    pub fn new(schema_provider: &'a dyn SchemaProvider) -> Self {
        Self { schema_provider }
    }

    /// Translate a Program to a Substrait Plan
    pub fn translate(&self, program: &Program) -> Result<Plan, TranslateError> {
        // Translate the main pipeline to a relation
        let root_rel = self.translate_pipeline(&program.pipeline)?;

        // Extract column names from the source for the root
        let names = self.get_output_names(&program.pipeline.source)?;

        // Wrap in PlanRel
        let plan_rel = substrait::proto::PlanRel {
            rel_type: Some(substrait::proto::plan_rel::RelType::Root(
                substrait::proto::RelRoot {
                    input: Some(root_rel),
                    names, // Output column names
                },
            )),
        };

        let plan = Plan {
            version: Some(substrait::proto::Version {
                minor_number: 53,
                patch_number: 0,
                ..Default::default()
            }),
            relations: vec![plan_rel],
            ..Default::default()
        };

        Ok(plan)
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

    fn translate_pipeline(&self, pipeline: &Pipeline) -> Result<substrait::proto::Rel, TranslateError> {
        // Start with the source and get the initial schema
        let mut rel = self.translate_source(&pipeline.source)?;

        // Get the schema context from the source
        let mut current_schema = self.get_output_names(&pipeline.source)?;

        // Apply operators on top of the source relation
        for op in &pipeline.ops {
            rel = self.translate_operator(op, rel, &current_schema)?;
            // TODO: Update current_schema if operator changes column set (e.g., Select)
        }

        Ok(rel)
    }

    fn translate_operator(&self, op: &Operator, input: substrait::proto::Rel, schema: &[String]) -> Result<substrait::proto::Rel, TranslateError> {
        match op {
            Operator::Filter { condition } => self.translate_filter(input, condition, schema),
            Operator::Select { projections } => self.translate_select(input, projections, schema),
            Operator::Sort { keys } => self.translate_sort(input, keys, schema),
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
            root_type: None, // Type inference
        };

        Ok(substrait::proto::Expression {
            rex_type: Some(substrait::proto::expression::RexType::Selection(Box::new(field_ref))),
        })
    }

    fn translate_binary_op(&self, op: &BinOp, left: &Expr, right: &Expr, schema: &[String]) -> Result<substrait::proto::Expression, TranslateError> {
        let left_expr = Box::new(self.translate_expr(left, schema)?);
        let right_expr = Box::new(self.translate_expr(right, schema)?);

        // Map MLQL binary operator to Substrait function name
        let function_name = match op {
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

        // Create scalar function call
        // Note: function_reference would need to be registered in an extension
        // For now, using a placeholder approach
        let scalar_function = substrait::proto::expression::ScalarFunction {
            function_reference: 0, // Would need proper function registration
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

        let function_name = match op {
            UnOp::Not => "not",
            UnOp::Neg => "negate",
        };

        // Create scalar function call
        let scalar_function = substrait::proto::expression::ScalarFunction {
            function_reference: 0,
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
}

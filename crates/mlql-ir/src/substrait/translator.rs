//! Core Substrait translator

use crate::{Program, Pipeline, Source};
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

        // Wrap in PlanRel
        let plan_rel = substrait::proto::PlanRel {
            rel_type: Some(substrait::proto::plan_rel::RelType::Root(
                substrait::proto::RelRoot {
                    input: Some(root_rel),
                    names: vec![], // Field names - empty for now
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

    fn translate_pipeline(&self, pipeline: &Pipeline) -> Result<substrait::proto::Rel, TranslateError> {
        // Start with the source
        let rel = self.translate_source(&pipeline.source)?;

        // TODO: Apply operators on top of the source relation
        for _op in &pipeline.ops {
            // Phase 2.2+: Implement Filter, Select, Sort, etc.
        }

        Ok(rel)
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

        println!("✅ Substrait plan generation test passed");
        println!("   Generated {} bytes", plan_bytes.len());
    }
}

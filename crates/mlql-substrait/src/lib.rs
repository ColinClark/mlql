//! Substrait encoder for MLQL IR
//!
//! Converts the canonical JSON IR to Substrait logical plans.

use mlql_ir::{Operator, Pipeline, Program};
use prost_reflect::DescriptorPool;
use std::sync::LazyLock;
use thiserror::Error;

// Descriptor pool for runtime reflection
pub static DESCRIPTOR_POOL: LazyLock<DescriptorPool> = LazyLock::new(|| {
    DescriptorPool::decode(
        include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin")).as_ref()
    )
    .expect("Failed to decode descriptor pool")
});

// Include generated protobuf types
#[allow(clippy::all)]
#[allow(non_snake_case)]
#[allow(warnings)]
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/substrait.rs"));

    // Include extensions module
    pub mod extensions {
        include!(concat!(env!("OUT_DIR"), "/substrait.extensions.rs"));
    }
}

// Re-export for convenience
pub use proto::*;

#[derive(Debug, Error)]
pub enum EncodingError {
    #[error("Unsupported operator: {0}")]
    UnsupportedOperator(String),

    #[error("Invalid type: {0}")]
    InvalidType(String),

    #[error("Missing schema information")]
    MissingSchema,

    #[error("Substrait encoding failed: {0}")]
    EncodingFailed(String),
}

pub struct SubstraitEncoder {
    #[allow(dead_code)]
    registry: mlql_registry::FunctionRegistry,
}

impl SubstraitEncoder {
    pub fn new() -> Self {
        Self {
            registry: mlql_registry::FunctionRegistry::new("0.58.0"),
        }
    }

    /// Encode a Program to Substrait JSON
    pub fn encode(&self, program: &Program) -> Result<String, EncodingError> {
        let plan = self.build_plan(program)?;

        // Serialize to binary protobuf first
        let mut buf = Vec::new();
        prost::Message::encode(&plan, &mut buf)
            .map_err(|e| EncodingError::EncodingFailed(e.to_string()))?;

        // Deserialize to DynamicMessage using descriptor pool
        let msg_desc = DESCRIPTOR_POOL.get_message_by_name("substrait.Plan")
            .ok_or_else(|| EncodingError::EncodingFailed("Plan message not found".to_string()))?;

        let dynamic_msg = prost_reflect::DynamicMessage::decode(msg_desc, buf.as_slice())
            .map_err(|e| EncodingError::EncodingFailed(e.to_string()))?;

        // Serialize to JSON
        let json = serde_json::to_string_pretty(&dynamic_msg)
            .map_err(|e| EncodingError::EncodingFailed(e.to_string()))?;

        Ok(json)
    }

    /// Encode to binary protobuf format
    pub fn encode_binary(&self, program: &Program) -> Result<Vec<u8>, EncodingError> {
        let plan = self.build_plan(program)?;

        let mut buf = Vec::new();
        prost::Message::encode(&plan, &mut buf)
            .map_err(|e| EncodingError::EncodingFailed(e.to_string()))?;

        Ok(buf)
    }

    fn build_plan(&self, program: &Program) -> Result<Plan, EncodingError> {
        // Build extension URIs
        let extensions = self.build_extensions(program)?;

        // Convert pipeline to Rel tree
        let rel = self.encode_pipeline(&program.pipeline)?;

        // Wrap in Plan message
        Ok(Plan {
            version: Some(Version {
                minor_number: 58,  // Substrait 0.58
                patch_number: 0,
                ..Default::default()
            }),
            #[allow(deprecated)]
            extension_uris: extensions.uris,
            extension_urns: vec![],  // URNs (newer format)
            extensions: extensions.functions,
            type_aliases: vec![],  // Custom type aliases
            relations: vec![PlanRel {
                rel_type: Some(plan_rel::RelType::Root(RelRoot {
                    input: Some(rel),
                    names: vec![],  // Field names
                })),
            }],
            advanced_extensions: None,
            expected_type_urls: vec![],
            parameter_bindings: vec![],
        })
    }

    fn build_extensions(&self, _program: &Program) -> Result<Extensions, EncodingError> {
        // TODO: Scan program for functions and build extension URIs
        Ok(Extensions {
            uris: vec![],
            functions: vec![],
        })
    }

    fn encode_pipeline(&self, pipeline: &Pipeline) -> Result<Rel, EncodingError> {
        // Start with source (ReadRel)
        let mut rel = self.encode_source(&pipeline.source)?;

        // Apply operators in sequence
        for op in &pipeline.ops {
            rel = self.encode_operator(op, rel)?;
        }

        Ok(rel)
    }

    fn encode_source(&self, source: &mlql_ir::Source) -> Result<Rel, EncodingError> {
        // Convert source to ReadRel
        let table_name = match source {
            mlql_ir::Source::Table { name, .. } => name.clone(),
            _ => return Err(EncodingError::UnsupportedOperator("non-table source".to_string())),
        };

        let read_rel = ReadRel {
            common: None,
            base_schema: Some(NamedStruct {
                names: vec![],  // TODO: Get from schema
                r#struct: None,
            }),
            filter: None,
            best_effort_filter: None,
            projection: None,
            advanced_extension: None,
            read_type: Some(read_rel::ReadType::NamedTable(read_rel::NamedTable {
                names: vec![table_name],
                advanced_extension: None,
            })),
        };

        Ok(Rel {
            rel_type: Some(rel::RelType::Read(Box::new(read_rel))),
        })
    }

    fn encode_operator(
        &self,
        op: &Operator,
        input: Rel,
    ) -> Result<Rel, EncodingError> {
        match op {
            Operator::Select { projections } => {
                // Build ProjectRel
                let project_rel = ProjectRel {
                    common: None,
                    input: Some(Box::new(input)),
                    expressions: projections.iter().map(|proj| {
                        match proj {
                            mlql_ir::Projection::Expr(expr) => self.encode_expr(expr),
                            mlql_ir::Projection::Aliased { expr, alias: _ } => {
                                // TODO: Handle aliases
                                self.encode_expr(expr)
                            }
                        }
                    }).collect::<Result<Vec<_>, _>>()?,
                    advanced_extension: None,
                };

                Ok(Rel {
                    rel_type: Some(rel::RelType::Project(Box::new(project_rel))),
                })
            }
            Operator::Filter { condition: _ } => {
                // TODO: Build FilterRel
                Err(EncodingError::UnsupportedOperator("filter".to_string()))
            }
            Operator::Join { source: _, on: _, join_type: _ } => {
                // TODO: Build JoinRel
                Err(EncodingError::UnsupportedOperator("join".to_string()))
            }
            Operator::GroupBy { keys: _, aggs: _ } => {
                // TODO: Build AggregateRel
                Err(EncodingError::UnsupportedOperator("groupby".to_string()))
            }
            Operator::Sort { keys: _ } => {
                // TODO: Build SortRel
                Err(EncodingError::UnsupportedOperator("sort".to_string()))
            }
            Operator::Take { limit: _ } => {
                // TODO: Build FetchRel
                Err(EncodingError::UnsupportedOperator("take".to_string()))
            }
            Operator::Knn { .. } => {
                // TODO: Build custom extension for KNN
                Err(EncodingError::UnsupportedOperator("knn".to_string()))
            }
            _ => Err(EncodingError::UnsupportedOperator("unknown".to_string())),
        }
    }

    fn encode_expr(&self, expr: &mlql_ir::Expr) -> Result<Expression, EncodingError> {
        match expr {
            mlql_ir::Expr::Column { col: _ } => {
                // Simple column reference
                Ok(Expression {
                    rex_type: Some(expression::RexType::Selection(Box::new(
                        expression::FieldReference {
                            reference_type: Some(
                                expression::field_reference::ReferenceType::DirectReference(
                                    expression::ReferenceSegment {
                                        reference_type: Some(
                                            expression::reference_segment::ReferenceType::StructField(
                                                Box::new(expression::reference_segment::StructField {
                                                    field: 0,  // TODO: Map column name to field index
                                                    child: None,
                                                })
                                            )
                                        ),
                                    }
                                )
                            ),
                            root_type: None,
                        }
                    ))),
                })
            }
            _ => Err(EncodingError::UnsupportedOperator(format!("expression: {:?}", expr))),
        }
    }
}

impl Default for SubstraitEncoder {
    fn default() -> Self {
        Self::new()
    }
}

// Helper struct for extension building
struct Extensions {
    uris: Vec<proto::extensions::SimpleExtensionUri>,
    functions: Vec<proto::extensions::SimpleExtensionDeclaration>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoder() {
        // TODO: Add encoding tests
    }
}

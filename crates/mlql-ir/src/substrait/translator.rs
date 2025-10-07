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
        // For now, just create an empty plan structure
        // We'll implement the actual translation in subsequent steps

        let plan = Plan {
            version: Some(substrait::proto::Version {
                minor_number: 53,
                patch_number: 0,
                ..Default::default()
            }),
            ..Default::default()
        };

        Ok(plan)
    }

    fn translate_pipeline(&self, pipeline: &Pipeline) -> Result<substrait::proto::Rel, TranslateError> {
        // Start with the source
        let _schema = self.translate_source(&pipeline.source)?;

        // TODO: Apply operators

        // Return placeholder for now
        Err(TranslateError::Translation("Not yet implemented".to_string()))
    }

    fn translate_source(&self, source: &Source) -> Result<(), TranslateError> {
        match source {
            Source::Table { name, alias: _ } => {
                // Look up schema
                let _schema = self.schema_provider
                    .get_table_schema(name)
                    .map_err(TranslateError::Schema)?;
                Ok(())
            }
            _ => Err(TranslateError::UnsupportedOperator("Only Table sources supported currently".to_string())),
        }
    }
}

//! Type system for MLQL IR

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    // Primitives
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Decimal { precision: u8, scale: u8 },

    // Text
    String,
    Varchar(Option<u32>),

    // Binary
    Blob,

    // Temporal
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Interval,

    // Complex
    Array(Box<DataType>),
    Struct(Vec<FieldType>),
    Map { key: Box<DataType>, value: Box<DataType> },

    // ML-specific
    Vector(Option<usize>), // Optional dimension

    // Special
    Null,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldType {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub fields: Vec<FieldType>,
}

impl Schema {
    pub fn new(fields: Vec<FieldType>) -> Self {
        Self { fields }
    }

    pub fn find_field(&self, name: &str) -> Option<&FieldType> {
        self.fields.iter().find(|f| f.name == name)
    }
}

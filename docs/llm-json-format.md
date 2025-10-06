# LLM JSON Output Format

## Overview

LLMs should emit strict JSON in the MLQL IR format. This makes output easier to:
- **Validate**: JSON schema validation
- **Repair**: Structured error correction
- **Cache**: Deterministic fingerprinting

## Contract

**Input**: User question + schema snapshot

**Output**: Strict JSON MLQL IR

## JSON Schema

### Basic Structure

```json
{
  "pragma": {
    "options": {
      "budget": {"rows_out": 1000}
    }
  },
  "pipeline": {
    "source": {
      "type": "Table",
      "name": "users",
      "alias": "u"
    },
    "ops": [...]
  }
}
```

### Expressions

All expressions use tagged unions with `"type"` field:

#### Column Reference
```json
{
  "type": "Column",
  "col": {
    "table": "u",
    "column": "age"
  }
}
```

#### Literal
```json
{
  "type": "Literal",
  "value": 25
}
```

#### Binary Operation
```json
{
  "type": "BinaryOp",
  "op": "Gt",
  "left": {
    "type": "Column",
    "col": {"column": "age"}
  },
  "right": {
    "type": "Literal",
    "value": 25
  }
}
```

#### Function Call
```json
{
  "type": "FuncCall",
  "func": "upper",
  "args": [{
    "type": "Column",
    "col": {"column": "name"}
  }]
}
```

### Operators

All operators use tagged unions with `"op"` field:

#### Select
```json
{
  "op": "Select",
  "projections": [
    {
      "type": "Column",
      "col": {"column": "name"}
    },
    {
      "expr": {
        "type": "BinaryOp",
        "op": "Mul",
        "left": {"type": "Column", "col": {"column": "age"}},
        "right": {"type": "Literal", "value": 2}
      },
      "alias": "double_age"
    }
  ]
}
```

#### Filter
```json
{
  "op": "Filter",
  "condition": {
    "type": "BinaryOp",
    "op": "And",
    "left": {
      "type": "BinaryOp",
      "op": "Gt",
      "left": {"type": "Column", "col": {"column": "age"}},
      "right": {"type": "Literal", "value": 25}
    },
    "right": {
      "type": "BinaryOp",
      "op": "Lt",
      "left": {"type": "Column", "col": {"column": "age"}},
      "right": {"type": "Literal", "value": 40}
    }
  }
}
```

#### Sort
```json
{
  "op": "Sort",
  "keys": [
    {
      "expr": {"type": "Column", "col": {"column": "age"}},
      "desc": true
    }
  ]
}
```

#### Take (LIMIT)
```json
{
  "op": "Take",
  "limit": 10
}
```

## Complete Examples

### Example 1: Simple Filter and Projection
**MLQL**: `from users | filter age > 25 | select [name, age]`

**JSON IR**:
```json
{
  "pipeline": {
    "source": {
      "type": "Table",
      "name": "users"
    },
    "ops": [
      {
        "op": "Filter",
        "condition": {
          "type": "BinaryOp",
          "op": "Gt",
          "left": {
            "type": "Column",
            "col": {"column": "age"}
          },
          "right": {
            "type": "Literal",
            "value": 25
          }
        }
      },
      {
        "op": "Select",
        "projections": [
          {
            "type": "Column",
            "col": {"column": "name"}
          },
          {
            "type": "Column",
            "col": {"column": "age"}
          }
        ]
      }
    ]
  }
}
```

### Example 2: Aggregation by Region
**Query**: "Show me total revenue by product in the EU region"

**JSON IR**:
```json
{
  "pragma": {
    "options": {
      "budget": {"rows_out": 1000}
    }
  },
  "pipeline": {
    "source": {
      "type": "Table",
      "name": "sales",
      "alias": "s"
    },
    "ops": [
      {
        "op": "Filter",
        "condition": {
          "type": "BinaryOp",
          "op": "Eq",
          "left": {
            "type": "Column",
            "col": {
              "table": "s",
              "column": "region"
            }
          },
          "right": {
            "type": "Literal",
            "value": "EU"
          }
        }
      },
      {
        "op": "GroupBy",
        "keys": [
          {
            "table": "s",
            "column": "product_id"
          }
        ],
        "aggs": {
          "revenue": {
            "func": "sum",
            "args": [
              {
                "type": "BinaryOp",
                "op": "Mul",
                "left": {
                  "type": "Column",
                  "col": {"table": "s", "column": "price"}
                },
                "right": {
                  "type": "Column",
                  "col": {"table": "s", "column": "qty"}
                }
              }
            ]
          }
        }
      }
    ]
  }
}
```

### Example 3: Complex Filters
**MLQL**: `from users | filter (age > 25 && age < 40) || name like "A%"`

**JSON IR**:
```json
{
  "pipeline": {
    "source": {
      "type": "Table",
      "name": "users"
    },
    "ops": [
      {
        "op": "Filter",
        "condition": {
          "type": "BinaryOp",
          "op": "Or",
          "left": {
            "type": "BinaryOp",
            "op": "And",
            "left": {
              "type": "BinaryOp",
              "op": "Gt",
              "left": {"type": "Column", "col": {"column": "age"}},
              "right": {"type": "Literal", "value": 25}
            },
            "right": {
              "type": "BinaryOp",
              "op": "Lt",
              "left": {"type": "Column", "col": {"column": "age"}},
              "right": {"type": "Literal", "value": 40}
            }
          },
          "right": {
            "type": "BinaryOp",
            "op": "Like",
            "left": {"type": "Column", "col": {"column": "name"}},
            "right": {"type": "Literal", "value": "A%"}
          }
        }
      }
    ]
  }
}
```

## Binary Operators Reference

| MLQL | JSON | SQL |
|------|------|-----|
| `+` | `"Add"` | `+` |
| `-` | `"Sub"` | `-` |
| `*` | `"Mul"` | `*` |
| `/` | `"Div"` | `/` |
| `%` | `"Mod"` | `%` |
| `==` | `"Eq"` | `=` |
| `!=` | `"Ne"` | `!=` |
| `<` | `"Lt"` | `<` |
| `<=` | `"Le"` | `<=` |
| `>` | `"Gt"` | `>` |
| `>=` | `"Ge"` | `>=` |
| `&&` | `"And"` | `AND` |
| `||` | `"Or"` | `OR` |
| `like` | `"Like"` | `LIKE` |
| `ilike` | `"ILike"` | `ILIKE` |

## Unary Operators Reference

| MLQL | JSON | SQL |
|------|------|-----|
| `-` | `"Neg"` | `-` |
| `!` | `"Not"` | `NOT` |

## Why JSON IR for LLMs?

1. **Validation**: Use JSON schema to validate before execution
2. **Repair**: Structured errors make fixing easier
3. **Caching**: Deterministic fingerprinting via SHA-256
4. **Provenance**: Track query origin and transformations
5. **Safety**: Type-checked before SQL generation

## LLM Prompt Template

```
Given this database schema:
{schema_json}

Generate a valid MLQL JSON IR query for: {user_question}

Output only valid JSON in this format:
{
  "pipeline": {
    "source": {"type": "Table", "name": "..."},
    "ops": [...]
  }
}
```

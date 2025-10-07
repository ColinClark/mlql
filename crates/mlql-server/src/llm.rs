//! OpenAI LLM integration for natural language to MLQL IR conversion

use async_openai::{
    types::{
        ChatCompletionRequestMessage, ChatCompletionRequestSystemMessageArgs,
        ChatCompletionRequestUserMessageArgs, CreateChatCompletionRequestArgs,
    },
    Client, config::OpenAIConfig,
};
use mlql_ir::Pipeline;

/// System prompt for OpenAI - teaches it to generate MLQL IR
const SYSTEM_PROMPT: &str = r#"You are an expert at converting natural language queries into MLQL IR (Intermediate Representation) in JSON format.

MLQL IR Format:
```json
{
  "pipeline": {
    "source": {
      "type": "Table",
      "name": "table_name"
    },
    "ops": [
      // Array of operators
    ]
  }
}
```

Available Operators:

1. Filter (WHERE clause):
```json
{
  "op": "Filter",
  "condition": {
    "type": "BinaryOp",
    "op": "Gt",  // Gt, Lt, Eq, Ne, Ge, Le, And, Or, Like
    "left": {"type": "Column", "col": {"column": "age"}},
    "right": {"type": "Literal", "value": 25}
  }
}
```

2. Select (projection):
```json
{
  "op": "Select",
  "projections": [
    {"type": "Column", "col": {"column": "name"}},
    {
      "type": "BinaryOp",
      "op": "Mul",
      "left": {"type": "Column", "col": {"column": "age"}},
      "right": {"type": "Literal", "value": 2},
      "alias": "double_age"
    }
  ]
}
```

3. Sort (ORDER BY):
```json
{
  "op": "Sort",
  "keys": [
    {"expr": {"type": "Column", "col": {"column": "age"}}, "desc": false}
  ]
}
```

4. Take (LIMIT):
```json
{
  "op": "Take",
  "limit": 10
}
```

5. Distinct:
```json
{
  "op": "Distinct"
}
```

6. GroupBy (with aggregates):
```json
{
  "op": "GroupBy",
  "keys": [{"column": "city"}],
  "aggs": {
    "total": {
      "func": "count",
      "args": []
    },
    "avg_price": {
      "func": "avg",
      "args": [{"type": "Column", "col": {"column": "price"}}]
    }
  }
}
```

7. Join:
```json
{
  "op": "Join",
  "source": {
    "type": "Table",
    "name": "orders",
    "alias": "o"
  },
  "on": {
    "type": "BinaryOp",
    "op": "Eq",
    "left": {"type": "Column", "col": {"table": "u", "column": "id"}},
    "right": {"type": "Column", "col": {"table": "o", "column": "user_id"}}
  },
  "join_type": "Inner"  // Inner, Left, Right, Full, Cross
}
```

Binary Operators:
- Arithmetic: Add, Sub, Mul, Div, Mod
- Comparison: Eq, Ne, Lt, Gt, Le, Ge, Like
- Logical: And, Or

Important Rules:
1. Always return ONLY valid JSON - no markdown, no explanations
2. Column references use {"type": "Column", "col": {"column": "name"}}
3. Literals use {"type": "Literal", "value": <value>}
4. For joins, use table aliases in column references
5. Aggregate functions: count, sum, avg, min, max

Examples:

Query: "Show me all users over 25"
Response:
{
  "pipeline": {
    "source": {"type": "Table", "name": "users"},
    "ops": [
      {
        "op": "Filter",
        "condition": {
          "type": "BinaryOp",
          "op": "Gt",
          "left": {"type": "Column", "col": {"column": "age"}},
          "right": {"type": "Literal", "value": 25}
        }
      }
    ]
  }
}

Query: "Get names and ages of users, sorted by age, limit 10"
Response:
{
  "pipeline": {
    "source": {"type": "Table", "name": "users"},
    "ops": [
      {
        "op": "Select",
        "projections": [
          {"type": "Column", "col": {"column": "name"}},
          {"type": "Column", "col": {"column": "age"}}
        ]
      },
      {
        "op": "Sort",
        "keys": [{"expr": {"type": "Column", "col": {"column": "age"}}, "desc": false}]
      },
      {
        "op": "Take",
        "limit": 10
      }
    ]
  }
}

Query: "Count users by city"
Response:
{
  "pipeline": {
    "source": {"type": "Table", "name": "users"},
    "ops": [
      {
        "op": "GroupBy",
        "keys": [{"column": "city"}],
        "aggs": {
          "total": {"func": "count", "args": []}
        }
      }
    ]
  }
}

Query: "how many users are there"
Response:
{
  "pipeline": {
    "source": {"type": "Table", "name": "users"},
    "ops": [
      {
        "op": "GroupBy",
        "keys": [],
        "aggs": {
          "count": {"func": "count", "args": []}
        }
      }
    ]
  }
}

Query: "show me the top 5 products by price"
Response:
{
  "pipeline": {
    "source": {"type": "Table", "name": "products"},
    "ops": [
      {
        "op": "Sort",
        "keys": [{"expr": {"type": "Column", "col": {"column": "price"}}, "desc": true}]
      },
      {
        "op": "Take",
        "limit": 5
      }
    ]
  }
}

Query: "get all orders from last month"
Response:
{
  "pipeline": {
    "source": {"type": "Table", "name": "orders"},
    "ops": [
      {
        "op": "Filter",
        "condition": {
          "type": "BinaryOp",
          "op": "Ge",
          "left": {"type": "Column", "col": {"column": "order_date"}},
          "right": {"type": "Literal", "value": "2024-01-01"}
        }
      }
    ]
  }
}

Query: "average price by category"
Response:
{
  "pipeline": {
    "source": {"type": "Table", "name": "products"},
    "ops": [
      {
        "op": "GroupBy",
        "keys": [{"column": "category"}],
        "aggs": {
          "avg_price": {
            "func": "avg",
            "args": [{"type": "Column", "col": {"column": "price"}}]
          }
        }
      }
    ]
  }
}

Query: "users and their orders"
Response:
{
  "pipeline": {
    "source": {"type": "Table", "name": "users", "alias": "u"},
    "ops": [
      {
        "op": "Join",
        "source": {"type": "Table", "name": "orders", "alias": "o"},
        "on": {
          "type": "BinaryOp",
          "op": "Eq",
          "left": {"type": "Column", "col": {"table": "u", "column": "id"}},
          "right": {"type": "Column", "col": {"table": "o", "column": "user_id"}}
        },
        "join_type": "Inner"
      }
    ]
  }
}

Query: "distinct cities"
Response:
{
  "pipeline": {
    "source": {"type": "Table", "name": "users"},
    "ops": [
      {
        "op": "Select",
        "projections": [{"type": "Column", "col": {"column": "city"}}]
      },
      {
        "op": "Distinct"
      }
    ]
  }
}

Return ONLY the JSON, no other text."#;

/// Convert natural language query to MLQL IR using OpenAI with error retry loop
#[allow(dead_code)]
pub async fn natural_language_to_ir(
    client: &Client<OpenAIConfig>,
    query: &str,
) -> Result<Pipeline, Box<dyn std::error::Error>> {
    natural_language_to_ir_with_catalog(client, query, None).await
}

/// Convert natural language query to MLQL IR using OpenAI with optional catalog context
pub async fn natural_language_to_ir_with_catalog(
    client: &Client<OpenAIConfig>,
    query: &str,
    catalog_json: Option<&str>,
) -> Result<Pipeline, Box<dyn std::error::Error>> {
    const MAX_RETRIES: usize = 3;

    // Build system prompt with optional catalog
    let system_prompt = if let Some(catalog) = catalog_json {
        format!("{}\n\n## Database Catalog\n\nThe following tables are available in the database. Use this information to construct accurate queries:\n\n{}", SYSTEM_PROMPT, catalog)
    } else {
        SYSTEM_PROMPT.to_string()
    };

    let mut messages = vec![
        ChatCompletionRequestMessage::System(
            ChatCompletionRequestSystemMessageArgs::default()
                .content(system_prompt)
                .build()?,
        ),
        ChatCompletionRequestMessage::User(
            ChatCompletionRequestUserMessageArgs::default()
                .content(query)
                .build()?,
        ),
    ];

    for attempt in 0..MAX_RETRIES {
        // Build chat completion request
        let request = CreateChatCompletionRequestArgs::default()
            .model("gpt-4o-mini")
            .messages(messages.clone())
            .temperature(0.0) // Deterministic output
            .build()?;

        // Call OpenAI API
        let response = client.chat().create(request).await?;

        // Extract response content
        let content = response
            .choices
            .first()
            .and_then(|choice| choice.message.content.as_ref())
            .ok_or("No response from OpenAI")?;

        tracing::info!("LLM Response (attempt {}): {}", attempt + 1, content);

        // Try to parse JSON response - first try as wrapper with "pipeline" key
        let pipeline_result = serde_json::from_str::<serde_json::Value>(content)
            .ok()
            .and_then(|v| {
                // Try to extract pipeline from wrapper
                if let Some(pipeline_value) = v.get("pipeline") {
                    match serde_json::from_value::<Pipeline>(pipeline_value.clone()) {
                        Ok(p) => Some(p),
                        Err(e) => {
                            tracing::warn!("Failed to parse pipeline from wrapper: {}", e);
                            tracing::warn!("Attempted to parse: {}", pipeline_value);
                            None
                        }
                    }
                } else {
                    // Try parsing whole thing as Pipeline directly
                    match serde_json::from_value::<Pipeline>(v.clone()) {
                        Ok(p) => Some(p),
                        Err(e) => {
                            tracing::warn!("Failed to parse as direct Pipeline: {}", e);
                            tracing::warn!("Attempted to parse: {}", v);
                            None
                        }
                    }
                }
            });

        match pipeline_result {
            Some(pipeline) => {
                // Success! Return the pipeline
                return Ok(pipeline);
            }
            None => {
                if attempt == MAX_RETRIES - 1 {
                    // Last attempt failed, return error
                    return Err(format!(
                        "Failed to parse MLQL IR after {} attempts. Response: {}",
                        MAX_RETRIES, content
                    )
                    .into());
                }

                // Add error feedback to conversation and retry
                messages.push(ChatCompletionRequestMessage::Assistant(
                    async_openai::types::ChatCompletionRequestAssistantMessageArgs::default()
                        .content(content.clone())
                        .build()?,
                ));
                let error_msg = format!(
                    "Error: Failed to parse your response as valid MLQL IR JSON. \
                     Please fix the JSON and try again. Remember: return ONLY valid JSON, no markdown formatting. \
                     Your response was: {}", content
                );
                tracing::warn!("Parse attempt {} failed, sending feedback to LLM", attempt + 1);

                messages.push(ChatCompletionRequestMessage::User(
                    ChatCompletionRequestUserMessageArgs::default()
                        .content(error_msg)
                        .build()?,
                ));
                // Continue to next retry
            }
        }
    }

    Err("Exceeded maximum retries".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_prompt_contains_examples() {
        assert!(SYSTEM_PROMPT.contains("Filter"));
        assert!(SYSTEM_PROMPT.contains("Select"));
        assert!(SYSTEM_PROMPT.contains("GroupBy"));
        assert!(SYSTEM_PROMPT.contains("Join"));
    }
}

#!/bin/bash
# Test Substrait execution by sending IR directly to the server

# The IR for: from bank_failures | sort -"Assets ($mil.)" | take 10
IR_JSON='{
  "source": {
    "type": "Table",
    "name": "bank_failures"
  },
  "ops": [
    {
      "op": "Sort",
      "keys": [
        {
          "expr": {
            "type": "Column",
            "col": {
              "column": "Assets ($mil.)"
            }
          },
          "desc": true
        }
      ]
    },
    {
      "op": "Take",
      "limit": 10
    }
  ]
}'

echo "Testing Substrait execution with IR:"
echo "$IR_JSON" | jq '.'

echo ""
echo "Sending request to MCP server..."
echo ""

# Call the MCP query tool with direct IR
# Note: This would need to be adapted based on how your MCP server accepts IR
# For now, let's create a direct HTTP test

curl -X POST http://localhost:8080/query-ir \
  -H "Content-Type: application/json" \
  -d "{\"ir\": $IR_JSON, \"database\": \"data/demo.duckdb\"}" \
  2>/dev/null | jq '.'

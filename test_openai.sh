#!/bin/bash

# Load API key from .env
export OPENAI_API_KEY=$(grep OPENAI_API_KEY /Users/colin/Dev/truepop/mlql/mlql-rs/.env | cut -d'=' -f2)

echo "Testing OpenAI API with key: ${OPENAI_API_KEY:0:20}..."
echo ""

# Model list (cheap, quick)
echo "=== Testing model list ==="
curl -s https://api.openai.com/v1/models \
  -H "Authorization: Bearer $OPENAI_API_KEY" | head -20

echo ""
echo ""

# Minimal chat call against gpt-4o-mini
echo "=== Testing chat completion with gpt-4o-mini ==="
curl -s https://api.openai.com/v1/chat/completions \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-4o-mini",
    "messages": [{"role":"user","content":"ping"}],
    "max_tokens": 5
  }' | python3 -m json.tool

echo ""

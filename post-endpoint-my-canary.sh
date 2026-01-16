#!/usr/bin/env bash
set -euo pipefail

curl -X POST \
  -H "Content-Type: application/json" \
  -d @new-endpoint.json \
  http://localhost:8000/endpoints/my-canary

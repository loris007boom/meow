#!/usr/bin/env bash
set -euo pipefail
curl -X GET http://localhost:8000/endpoints > all-endpoints.json

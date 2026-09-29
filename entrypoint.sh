#!/bin/sh
set -eu

DB_PATH="${TO_DB_PATH:-/data/to_dashboard.db}"
mkdir -p "$(dirname "$DB_PATH")"

echo "[Data Stability Dashboard] DB: $DB_PATH"

PORT_VALUE="${PORT:-8501}"

exec streamlit run /app/app.py \
  --server.address=0.0.0.0 \
  --server.port="$PORT_VALUE" \
  --server.headless=true \
  --browser.gatherUsageStats=false

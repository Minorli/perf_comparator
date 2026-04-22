#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

cat >"$TMP_DIR/config.ini" <<EOF
[ORACLE_SOURCE]
user = demo
password = demo
dsn = 127.0.0.1:1521/ORCL

[OCEANBASE_TARGET]
executable = /bin/echo
host = 127.0.0.1
port = 2881
user_string = root@test#obcluster
password = secret

[SETTINGS]
source_schemas = APP
workloads_dir = $TMP_DIR/workloads
report_dir = $TMP_DIR/reports
EOF

cat >"$TMP_DIR/input.sql" <<'EOF'
SELECT * FROM orders WHERE status = 'ACTIVE';
SELECT * FROM customers WHERE customer_id = 1;
EOF

cd "$ROOT_DIR"

python3 perf_comparator.py --mode batch --config "$TMP_DIR/config.ini" --sql-file "$TMP_DIR/input.sql"

REPLAY_FILE="$(ls "$TMP_DIR"/workloads/replay_*.jsonl | head -n 1)"
python3 perf_comparator.py --mode report-only --config "$TMP_DIR/config.ini" --replay "$REPLAY_FILE"

echo "Smoke test completed successfully."

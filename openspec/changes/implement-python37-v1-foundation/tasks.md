## 1. Runtime Foundation

- [x] 1.1 Create the single-file Python 3.7 program skeleton in `perf_comparator.py`, with timestamped artifact utilities for workload, replay, audit, and report outputs.
- [x] 1.2 Pin Python 3.7-compatible dependencies, including `python-oracledb` 2.x, and document offline installation plus `obclient` prerequisites.
- [x] 1.3 Implement comparator-compatible INI parsing for `[ORACLE_SOURCE]` and `[OCEANBASE_TARGET]`, plus shared logging, run ID generation, and JSONL serialization helpers inside the single-file layout, with unit tests on Python 3.7.

## 2. Oracle Capture

- [x] 2.1 Implement Oracle capability probing for AWR, `V$SQL`, audit trail, WCR, and manual SQL file fallback, and persist `capture_capability_<ts>.json`.
- [x] 2.2 Implement batch workload capture with normalized SQL text, bind metadata, Oracle metrics, and Oracle plan rows using `python-oracledb`, and support `source_db_mode=oceanbase` source capture through `GV$OB_SQL_AUDIT`.
- [x] 2.3 Implement stream-mode watermark polling and schema filtering, with tests that verify append-only workload behavior.

## 3. OceanBase Replay Baseline

- [x] 3.1 Implement OceanBase replay preflight checks for `obclient`, connectivity, tenant settings, and replay capability persistence.
- [x] 3.2 Implement the `obclient`-backed replay adapter for timeout setup, SQL execution, status capture, and `EXPLAIN EXTENDED` collection, reusing comparator-style `user_string` login semantics.
- [x] 3.3 Implement typed bind literalization, unsupported bind classification, and explicit replay skip statuses for statements that cannot be represented safely.

## 4. Diagnostics and Correlation

- [x] 4.1 Implement the SQL Audit polling daemon logic under the same top-level Python program, with a default 300ms cadence and append-only `audit_dump_<ts>.jsonl` persistence.
- [x] 4.2 Implement correlation between replay attempts and SQL Audit rows using request identifiers, SQL identifiers, and execution timing windows.
- [x] 4.3 Define the replay backend interface and add a placeholder integration seam for a future OBCI-backed helper without changing artifact schemas.

## 5. Reporting and Rules

- [x] 5.1 Implement derived metrics such as speedup ratio, plan change, read amplification, and network ratio from replay artifacts.
- [x] 5.2 Implement the first rule set and generate HTML, text, and SQL hint outputs from replay JSONL.
- [x] 5.3 Add golden-file or fixture-based tests for report ranking, rule matching, and error classification outputs.

## 6. Validation and Documentation

- [x] 6.1 Add Python 3.7-compatible unit tests for config parsing, JSONL schema serialization, bind literalization, and rule evaluation.
- [x] 6.2 Add integration smoke scripts for `batch`, `replay-only`, and `report-only` flows with deterministic sample artifacts.
- [x] 6.3 Document deployment assumptions, comparator-aligned connection config, connector boundaries, Python 3.7 constraints, and the future OBCI path in project documentation.

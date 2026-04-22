## 1. Pipeline

- [x] 1.1 Add a `source-report` CLI mode for single-OB troubleshooting.
- [x] 1.2 Allow `source-report` configs to omit `OCEANBASE_TARGET` and Oracle sections.
- [x] 1.3 Add tests covering source-only config loading and CLI behavior.

## 2. Source Evidence

- [x] 2.1 Extend source SQL Audit capture to retain richer troubleshooting fields.
- [x] 2.2 Aggregate source workload rows into source-only diagnostic report rows.
- [x] 2.3 Add tests covering source aggregation and rule evaluation.

## 3. Reporting

- [x] 3.1 Generate HTML/TXT/SQL-hint outputs for source-only rows without Oracle comparison.
- [x] 3.2 Add source-aware evidence strings and summary language.
- [x] 3.3 Add fixture or report tests for source-only output.

## 4. Real Validation

- [x] 4.1 Construct a complex OB 4.2.5 source-side SQL and package workload.
- [x] 4.2 Run `source-report` against a single OB 4.2.5 source and capture evidence artifacts.
- [x] 4.3 Read the generated report and record whether it surfaced the expected problems.

## 1. Verification

- [x] 1.1 Add config and CLI controls for result-set verification and row sampling limits.
- [x] 1.2 Implement source/target row hashing for `SELECT` statements and bounded mismatch artifacts.
- [x] 1.3 Add tests for matching results, mismatches, and oversized result skips.

## 2. Plan Monitor

- [x] 2.1 Implement `GV$SQL_PLAN_MONITOR` collection for distributed or slow SQL.
- [x] 2.2 Parse operator-level monitor rows into structured replay evidence.
- [x] 2.3 Add tests for plan monitor parsing and evidence gating.

## 3. Reporting

- [x] 3.1 Surface verification status and mismatch references in replay records and reports.
- [x] 3.2 Surface plan-monitor summaries and evidence-backed rules in reports.
- [x] 3.3 Add fixture-based report tests covering verification and plan-monitor output.

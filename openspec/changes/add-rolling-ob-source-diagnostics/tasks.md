## 1. OpenSpec and Tests

- [x] 1.1 Add OpenSpec deltas for rolling OB source diagnostics.
- [x] 1.2 Add tests for richer source audit capture fields and caller attribution.
- [x] 1.3 Add tests for rolling source-report generation and SQL/PLSQL sections.

## 2. Source Capture and Aggregation

- [x] 2.1 Extend source audit capture to include caller attribution fields from `GV$OB_SQL_AUDIT`.
- [x] 2.2 Add caller-group aggregation and statement workload-type classification.
- [x] 2.3 Add rolling source-report refresh during long-running source-report mode.

## 3. Reporting and Validation

- [x] 3.1 Strengthen source-only reports with top caller groups and separate slow SQL / slow PL/SQL sections.
- [x] 3.2 Run targeted and full validation for the new rolling source-diagnostics workflow.

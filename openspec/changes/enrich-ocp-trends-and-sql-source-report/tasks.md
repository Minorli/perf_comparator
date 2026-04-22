## 1. OCP Trends

- [x] 1.1 Add tests for native OCP trend collection and resolved target identity in evidence.
- [x] 1.2 Implement native OCP trend fetch and include resolved cluster/tenant context in diagnostic payloads.

## 2. Source-Only Reporting

- [x] 2.1 Add tests for SQL text source distribution counts and chart output in source-only reports.
- [x] 2.2 Implement SQL source aggregation, summary annotations, and source-only chart rendering.

## 3. Docs And Validation

- [x] 3.1 Update README, runtime docs, and config guidance for OCP trends and SQL-source reporting.
- [x] 3.2 Run targeted tests, `python3 -m unittest -v`, and `openspec validate --type change enrich-ocp-trends-and-sql-source-report --strict --no-interactive`.

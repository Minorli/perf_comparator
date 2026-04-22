## 1. OpenSpec and Tests

- [x] 1.1 Add OpenSpec deltas for rolling Oracle-to-OB monitoring and SQL visibility warnings.
- [x] 1.2 Add tests for non-SYS OceanBase source visibility warnings.
- [x] 1.3 Add tests for rolling Oracle stream capture, replay, and report refresh.
- [x] 1.4 Add tests for replay report SQL / PL/SQL split sections.

## 2. Runtime

- [x] 2.1 Add prominent runtime warnings when OceanBase source capture is configured with a non-SYS login.
- [x] 2.2 Decouple Oracle capture breadth from report Top N with a dedicated capture limit.
- [x] 2.3 Add rolling Oracle stream orchestration that captures, replays new statements, and refreshes reports in-place.

## 3. Reporting and Docs

- [x] 3.1 Strengthen replay reports with separate slow SQL and slow PL/SQL sections.
- [x] 3.2 Strengthen source-only reports with prominent QUERY_SQL visibility warning blocks.
- [x] 3.3 Update config template and README/runtime docs for long-running Oracle->OB monitoring.
- [x] 3.4 Run targeted and full validation for the new live-monitoring workflow.

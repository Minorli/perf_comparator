## Why

The current `source-report` mode can capture OceanBase source workload and produce an after-the-fact report, but the production use case is stronger:

- the tool should run for hours in the background while multiple test teams exercise the tenant
- operators need rolling report updates during the run, not only at the end
- the report should answer which caller groups triggered slow SQL and PL/SQL, not only which SQL IDs were slow

Without caller attribution and rolling output, the tool is still too batch-oriented for production validation windows.

## What Changes

- Extend source-side audit capture to record caller attribution fields from `GV$OB_SQL_AUDIT`.
- Aggregate source-only workload by both statement and caller group signals.
- Add rolling source-report generation during long-running capture windows.
- Strengthen source-only reports with separate slow SQL / slow PL/SQL sections and top caller-group summaries.

## Capabilities

### Modified Capabilities

- `pipeline-orchestration`: source-only capture can update rolling reports while the run is still active.
- `performance-analysis-reporting`: source-only reports now highlight caller groups and separate SQL/PLSQL hotspots.

## Impact

- Affected code: `perf_comparator.py`, `test_perf_comparator.py`
- Runtime impact: rolling source-report mode regenerates a lightweight report snapshot on a configurable cadence
- Risks: rolling report generation must stay bounded and should avoid expensive deep diagnostics on every refresh

## Why

The current OceanBase-source support still assumes a comparison workflow:

- it captures OB SQL Audit rows as source workload,
- then replays them against another target,
- and reports only in a migration-comparison framing.

That does not satisfy the operational troubleshooting case where an engineer has only one OceanBase 4.2.5 environment and wants to:

- capture that environment's own workload,
- identify problematic SQL or package executions directly from source-side evidence,
- print a report for troubleshooting without any Oracle baseline or second OceanBase target.

## What Changes

- Add a standalone OceanBase-source diagnosis mode that uses only `OCEANBASE_SOURCE`.
- Extend source-side SQL Audit capture to retain richer fields needed for troubleshooting.
- Aggregate captured OB source workload into report rows without replay.
- Generate HTML/TXT/SQL-hint outputs oriented around source-side hotspots and bottlenecks.

## Capabilities

### New Capabilities

- `ob-source-standalone-diagnostics`: capture and analyze a single OceanBase source environment for troubleshooting.

### Modified Capabilities

- `pipeline-orchestration`: add a standalone source-report mode.
- `performance-analysis-reporting`: support source-only hotspot analysis without Oracle comparison.

## Impact

- Adds a new operator-facing workflow for OceanBase-only troubleshooting.
- Reuses the single-file runtime and existing report infrastructure.
- Keeps Oracle/target-comparison workflows intact while adding a clean source-only path.

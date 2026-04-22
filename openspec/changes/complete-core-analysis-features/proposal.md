## Why

The current implementation has a usable core workflow, but it still lacks several high-value capabilities needed for a production-grade performance analysis tool: operator-level evidence for distributed slow SQL, result-set verification, and richer reporting of those findings. These gaps reduce diagnostic confidence exactly in the cases where customers need the strongest proof.

## What Changes

- Add result-set verification for `SELECT` statements, using sorted row hashing and bounded mismatch sampling.
- Extend replay diagnostics to query `GV$SQL_PLAN_MONITOR` for distributed slow SQL and persist operator-level evidence.
- Enrich reports to surface plan monitor signals, result verification outcomes, and stronger action-oriented recommendations.
- Add tests for verification flows, plan monitor parsing, and richer report output.

## Capabilities

### New Capabilities
- `result-set-verification`: Verify source and replayed `SELECT` results with bounded hashing and mismatch evidence.

### Modified Capabilities
- `ob-replay-diagnostics`: Add plan-monitor evidence collection for distributed slow replays.
- `performance-analysis-reporting`: Add result verification outcomes and plan-monitor evidence to reports and recommendations.

## Impact

- Affects replay execution logic, report generation, and JSONL schemas.
- Adds optional runtime cost for verification and operator-level diagnostics, gated by config and thresholds.
- Keeps the single-file Python deployment model and existing artifact contracts while making analysis more trustworthy.

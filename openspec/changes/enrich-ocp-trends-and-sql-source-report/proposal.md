## Why

The tool can already use native OCP SQL lookup and multi-source SQL text recovery, but operators still lack three pieces of polish that matter in production: OCP trend evidence for matched SQL, explicit visibility into which cluster and tenant OCP evidence came from, and a visual view of SQL text recovery source mix in source-only reports.

## What Changes

- Extend native OCP diagnostics to fetch SQL trend data for matched SQL IDs.
- Persist resolved OCP cluster and tenant IDs into capability artifacts and report evidence when name-based resolution is used.
- Add SQL text source distribution metrics and a lightweight chart to source-only HTML reports.
- Surface SQL text source annotations more explicitly in summary and hints outputs.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `ob-replay-diagnostics`: native OCP evidence now includes SQL trends and resolved target identity context.
- `performance-analysis-reporting`: source-only reporting now exposes SQL text source distribution and richer OCP target context.

## Impact

- Affected code: `perf_comparator.py`, `test_perf_comparator.py`, docs, config template, README
- External systems: OCP `sqls/{sqlId}/trends` endpoint
- Risks: trend fetch failures must remain non-blocking, and additional source-report annotations must not break existing summary fixtures

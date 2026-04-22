## Why

The current profiler integration is usable, but still too raw for enterprise troubleshooting of complex packages:

- it shows hot lines, but not hot blocks or hot units
- it does not classify common PL/SQL performance anti-patterns
- recommendations still lean on generic row-by-row guidance
- reports do not clearly answer "which part of the package is slow, why, and what should I change first"

For migration and production tuning work, operators need profiler output that behaves more like a diagnostic product than a raw trace dump.

## What Changes

- Aggregate profiler evidence into unit-level summaries and contiguous hot blocks.
- Diagnose common slow-package patterns such as row-by-row SQL in loops, dynamic SQL inside loops, frequent commits, and tight CPU loops.
- Surface profiler diagnosis summaries in reports and recommendation output.
- Add complex synthetic package test cases that prove the tool can identify slow blocks and emit targeted guidance.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `ob-replay-diagnostics`: profiler evidence now includes unit summaries, hot blocks, and diagnosis metadata.
- `performance-analysis-reporting`: reports now present profiler findings as actionable package diagnostics instead of only raw hot lines.

## Impact

- Affected code: `perf_comparator.py`, `test_perf_comparator.py`
- Runtime impact: a few additional profiler aggregation queries when profiler mode is enabled
- Risk: added report detail must stay concise enough for operators while still exposing full diagnostic evidence in artifacts

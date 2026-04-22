## Why

The current PL/SQL profiler path assumes that `PLSQL_PROFILER_DATA.LINE#` can be joined directly to `ALL_SOURCE.LINE`. That assumption is too optimistic for OceanBase:

- some versions expose source text as line rows
- some environments expose package source as a single CLOB-like row that must be split by LF
- reports currently present the hottest line as if the mapping were always exact

This leaves operators with profiler evidence that looks precise even when the source mapping path was reconstructed or partially inferred.

## What Changes

- Detect OceanBase version with `SELECT OB_VERSION() FROM DUAL` and cache it for profiler evidence.
- Replace direct `ALL_SOURCE` line joins with an adaptive source loader that can use `DBA_SOURCE` or `ALL_SOURCE`, detect single-row source blobs, and split them into logical lines.
- Persist source-mapping strategy, source view, detected layout, and confidence together with profiler hot-line evidence.
- Surface profiler mapping confidence in report summaries, HTML evidence, and SQL hint comments so operators can judge whether a hot line is exact or reconstructed.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `ob-replay-diagnostics`: profiler evidence now includes version-aware source mapping metadata.
- `performance-analysis-reporting`: reports now surface profiler mapping confidence instead of implying every hot line is exact.

## Impact

- Affected code: `perf_comparator.py`, `test_perf_comparator.py`
- Runtime impact: a small amount of extra metadata probing and cached source loading for PL/SQL-profiled statements
- Risk: source reconstruction must preserve blank lines and avoid overstating precision

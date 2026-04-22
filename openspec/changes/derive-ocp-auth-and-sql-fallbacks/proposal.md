## Why

The tool still expects operators to prebuild `PERF_OCP_AUTH`, and OCP native diagnostics still require manual cluster and tenant IDs. SQL text recovery in source-only diagnosis also remains too narrow because it only tries local database views before giving up.

## What Changes

- Add native OCP Basic-auth generation from configured username and password.
- Support OCP cluster and tenant auto-resolution by name when IDs are not configured.
- Add a multi-source SQL text recovery strategy for source-only analysis: captured text, privileged OB lookup, native OCP lookup, then generic template fallback.
- Expose the chosen SQL text source and OCP auth mode in capability artifacts and reports.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `ob-replay-diagnostics`: native OCP collection now supports generated Basic auth and name-based target resolution.
- `performance-analysis-reporting`: source-only and replay reports now surface which SQL text recovery path succeeded when direct SQL text is unavailable.

## Impact

- Affected code: `perf_comparator.py`, `test_perf_comparator.py`, config template, runtime docs, README
- External systems: OCP `api/v2/ob/clusters` endpoint for name-to-id resolution
- Risks: credentials must be handled without leaking into process args, and SQL fallback order must preserve current low-latency local lookups before external requests

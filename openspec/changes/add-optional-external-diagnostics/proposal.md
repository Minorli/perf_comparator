## Why

The remaining `partial` audit items are real operator gaps: PL/SQL profiling still assumes profiler system tables already exist, and advanced external diagnostics promised by the design are not yet attachable to the workflow. The repository also lacks a root `README`, which makes deployment and mode selection unnecessarily opaque for first-time operators.

## What Changes

- Add first-use `DBMS_PROFILER.OB_INIT_OBJECTS(FALSE)` bootstrap with cached status and graceful fallback.
- Extend replay capability probing to surface optional profiler, OCP, and obdiag readiness.
- Add optional OCP fetch and obdiag bundle collection hooks for severe regressions, with artifact persistence and non-blocking failure handling.
- Surface external diagnostics status and artifact references in generated reports and capability files.
- Add a root `README.md` covering runtime constraints, configuration, modes, artifacts, and verification workflow.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `ob-replay-diagnostics`: replay capability probing and diagnostic collection now include profiler bootstrap readiness plus optional OCP and obdiag connectors.
- `performance-analysis-reporting`: reports now include external diagnostics evidence or degradation notes when optional OCP or obdiag collection is configured.

## Impact

- Affected code: `perf_comparator.py`, `test_perf_comparator.py`, config template, runtime docs, and new root `README.md`
- External systems: optional OCP HTTP endpoints and optional obdiag CLI installation
- Risks: misconfigured external diagnostics must not block replay or report generation; profiler initialization must be idempotent and safe on repeated runs

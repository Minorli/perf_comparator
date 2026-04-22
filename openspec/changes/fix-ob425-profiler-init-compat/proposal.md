## Why

Real-database validation against OceanBase `4.2.5.7` showed that the profiler runtime still has a compatibility gap:

- `CALL DBMS_PROFILER.OB_INIT_OBJECTS(FALSE)` fails with `ORA-00900`
- the profiler smoke therefore gets skipped even though `DBMS_PROFILER` is present and executable

This blocks real profiler evidence on exactly the OB version the product targets.

## What Changes

- Replace `CALL`-style profiler wrapper SQL with `BEGIN ... END;` blocks for initialization and start/stop operations.
- Add regression tests that lock the generated profiler SQL to the compatible form.
- Re-run real DB validation on OB 4.2.5 using the complex package profiler case.

## Capabilities

### Modified Capabilities

- `ob-replay-diagnostics`: profiler initialization and sampling become compatible with OceanBase 4.2.5.

## Impact

- Affected code: `perf_comparator.py`, `test_perf_comparator.py`
- Runtime impact: none beyond improved compatibility

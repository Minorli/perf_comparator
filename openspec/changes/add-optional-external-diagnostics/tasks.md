## 1. Profiler Bootstrap

- [x] 1.1 Add tests for lazy `DBMS_PROFILER.OB_INIT_OBJECTS(FALSE)` initialization and graceful downgrade on init failure.
- [x] 1.2 Implement cached profiler initialization status and wire it into PL/SQL replay collection.

## 2. External Diagnostics

- [x] 2.1 Add tests for replay capability probing with optional profiler, OCP, and obdiag readiness.
- [x] 2.2 Add tests for non-blocking OCP fetch and obdiag collection evidence in generated reports.
- [x] 2.3 Implement optional OCP fetch helpers, obdiag subprocess collection, and replay/report evidence plumbing.

## 3. Operator Docs

- [x] 3.1 Add a root `README.md` describing runtime constraints, configuration, modes, artifacts, and verification commands.
- [x] 3.2 Update config and runtime docs for profiler bootstrap and optional external diagnostics settings.
- [x] 3.3 Run `python3 -m py_compile perf_comparator.py test_perf_comparator.py`, `python3 -m unittest -v`, and `openspec validate --type change add-optional-external-diagnostics --strict --no-interactive`.

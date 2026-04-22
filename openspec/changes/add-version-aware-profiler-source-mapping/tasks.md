## 1. OpenSpec and Tests

- [x] 1.1 Add OpenSpec deltas for profiler source mapping and report confidence.
- [x] 1.2 Add tests covering OB version probing and profiler source-shape reconstruction.
- [x] 1.3 Add tests covering report output for profiler mapping confidence.

## 2. Profiler Source Mapping

- [x] 2.1 Implement cached OceanBase version probing with `OB_VERSION()`.
- [x] 2.2 Replace direct source joins with adaptive `DBA_SOURCE` / `ALL_SOURCE` loading and LF-based reconstruction.
- [x] 2.3 Persist mapping strategy, source view, layout, and confidence with profiler evidence.

## 3. Reporting and Validation

- [x] 3.1 Surface profiler mapping confidence in summary, HTML, and hint outputs.
- [x] 3.2 Run py_compile, unittest, and OpenSpec validation for the change.

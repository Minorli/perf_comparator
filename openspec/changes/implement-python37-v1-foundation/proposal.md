## Why

The product baseline is already clear, but the implementation path needs to match the actual server runtime: Python 3.7. We need a concrete v1 proposal that preserves the existing capture/replay/report architecture, stays deployable in customer intranets, and accounts for OceanBase's connector constraints instead of assuming a Python-native Oracle-mode driver exists.

## What Changes

- Add a Python 3.7 runtime foundation capability that fixes dependency policy, connector boundaries, and extension points for the first implementation.
- Constrain the implementation to a single distributable Python program so production rollout, hotfix delivery, and replacement are operationally simple.
- Standardize on `python-oracledb` 2.x for Oracle-side capture because it is compatible with Python 3.7.
- Use an `obclient`-based OceanBase replay baseline for Oracle-tenant execution and diagnostics, while keeping a future OBCI-backed native bridge behind an abstraction boundary.
- Reuse the established `~/comparator` connection config style so operators do not need to learn a new credential layout.
- Break the first implementation into staged tasks covering CLI/config, Oracle capture, OceanBase replay and SQL Audit collection, reporting, and validation.

## Capabilities

### New Capabilities
- `python37-runtime-foundation`: Define the Python 3.7-compatible runtime, connector strategy, and future OBCI extension boundary for implementing the existing v1 baseline.

### Modified Capabilities
None.

## Impact

- Affects the initial Python package layout, requirements pinning, and deployment instructions.
- Replaces a multi-module code layout with a single-file program layout centered on `perf_comparator.py`.
- Introduces connector-role separation between Oracle capture, OceanBase replay, and artifact persistence.
- Aligns connection configuration with `~/comparator` by reusing `[ORACLE_SOURCE]`, `[OCEANBASE_TARGET]`, and related field semantics.
- Preserves existing baseline capabilities under `openspec/specs/` without changing product goals or report outputs.
- Reduces implementation risk by avoiding a hard dependency on native OBCI integration in the first delivery while preserving a clean upgrade path.

## Why

The current implementation covers the core SQL capture, replay, verification, and report loop, but several requirements from the original design are still incomplete:

- PL/SQL and package regressions do not yet have line-level profiler evidence.
- Reports still summarize plan changes loosely instead of translating risky operator transitions explicitly.
- OceanBase-source mode still assumes Oracle config is present even when the workflow is OB -> OB only.
- There is no first-class real-database verification mode inside the single-file runtime, so live validation still depends on ad hoc manual steps.

These gaps make the tool less defensible in the exact scenarios customers care about most: package performance regressions, distributed-plan regressions, and production-like validation against real Oracle and OceanBase environments.

## What Changes

- Add optional DBMS_PROFILER-backed PL/SQL/package diagnostics for replayed anonymous blocks and package calls.
- Add operator-translation risk signals and richer plan-diff reporting.
- Allow OceanBase-source workflows to run without requiring `[ORACLE_SOURCE]`.
- Add a single-file `verify-realdb` mode that performs live Oracle/OB connectivity checks, Oracle->OB replay smoke validation, optional profiler-package validation, and OB-source capture smoke validation using reference configs.

## Capabilities

### Modified Capabilities

- `ob-replay-diagnostics`: add optional PL/SQL profiler evidence collection.
- `performance-analysis-reporting`: add operator-translation risk evidence and profiler-backed diagnostics.
- `pipeline-orchestration`: add live verification mode and support OceanBase-source operation without Oracle config.

### New Capabilities

- `runtime-validation`: provide a built-in real-database verification workflow with evidence artifacts.

## Impact

- Affects single-file runtime config loading, replay execution, and report generation.
- Adds optional runtime overhead only when profiler or live verification modes are enabled.
- Keeps the single-file Python deployment model and reuses comparator connection addresses only at runtime, without copying secrets into the repository.

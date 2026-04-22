## Context

The remaining gaps sit at the boundary between "usable tool" and "migration-grade diagnostic system":

- SQL-level evidence is already present, but package and anonymous block regressions still rely on heuristics.
- Plan-monitor evidence exists, but operator translation and plan-diff expression remain too shallow.
- Real DB validation is still manual and fragmented.
- OB-source-only workflows should not be forced to carry an Oracle config they never use.

The implementation must preserve these constraints:

- Python 3.7 compatible
- Single-file runtime
- Minimal additional dependencies
- Safe by default for real DB validation

## Goals / Non-Goals

**Goals**

- Add optional PL/SQL profiler collection with source-line evidence.
- Make plan-diff reporting operator-aware and risk-oriented.
- Add built-in real DB verification mode with structured evidence.
- Remove Oracle config as a hard requirement for OB-source-only workflows.

**Non-Goals**

- Full OCP integration in this change.
- Replacing the existing JSONL stage contracts wholesale.
- Shipping extra runtime helper programs beyond the single Python entrypoint.

## Decisions

### 1. Treat Oracle config as optional in OB-source-only mode

If `source_db_mode=oceanbase`, capture, replay, and report paths can run without Oracle connectivity. Oracle config should therefore become optional and only be required by behaviors that explicitly need it, such as Oracle-side result verification or Oracle connectivity smoke checks.

### 2. Add profiler collection as an optional deep-diagnostics path

DBMS_PROFILER is expensive and privilege-sensitive. It should only run when explicitly enabled and only for SQL text that looks like PL/SQL or package execution.

Implementation direction:

- Add config and CLI toggles for profiler mode.
- Wrap successful PL/SQL replay execution in DBMS_PROFILER start/stop calls when enabled.
- Query `PLSQL_PROFILER_RUNS`, `PLSQL_PROFILER_UNITS`, `PLSQL_PROFILER_DATA`, and `ALL_SOURCE` for top hot lines.
- Persist profiler evidence to `plsql_profile_<ts>.jsonl` and attach summaries to replay/report records.

### 3. Express plan changes as translated operator risk signals

Plan hash changes alone are too weak. The report should call out risky transitions explicitly.

Implementation direction:

- Add a translation table for high-risk Oracle -> OB operator changes.
- Compute plan-diff signals from Oracle and OB plan rows, optionally reinforced by plan-monitor spill evidence.
- Surface those signals in the summary, HTML report, and SQL hints.

### 4. Add a built-in `verify-realdb` mode

Real validation should live inside the same runtime instead of relying on external scripts.

Implementation direction:

- Use the current config when provided, and optionally import connection sections from reference configs such as `~/comparator/config.ini` and `~/comparator/config.ini.ob`.
- Perform safe connectivity probes.
- Run a deterministic Oracle -> OB replay smoke using a temporary SQL file and optional result verification.
- Optionally deploy and run a small profiler test package for package-level validation.
- Optionally trigger OB-source SQL Audit capture smoke by generating source-side traffic.
- Persist a machine-readable validation summary artifact.

## Risks / Trade-offs

- [Profiler privileges may be missing] -> degrade cleanly and record the failure in evidence instead of aborting the whole run.
- [Real DB verification can be intrusive if package deployment is automatic] -> keep package deployment opt-in.
- [Single-file runtime keeps growing] -> group helpers by feature and enforce behavior through tests.
- [OB-source SQL IDs may not match target SQL IDs] -> keep SQL-text fallback matching for reporting, but use source-mode-aware query strategies.

## Migration Plan

1. Make Oracle config optional for OB-source mode.
2. Add profiler config/CLI controls and profiler evidence helpers.
3. Add translated plan-diff risk signals and richer report sections.
4. Add `verify-realdb` mode and summary artifact.
5. Add unit tests and run live verification using comparator-derived connection addresses.

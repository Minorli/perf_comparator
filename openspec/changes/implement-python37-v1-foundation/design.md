## Context

`perf_comparator` already has a product-level baseline for capture, replay, diagnostics, and reporting. The remaining decision is how to implement that baseline on the actual deployment target, which is constrained to Python 3.7 on customer servers.

The official connector landscape is uneven:

- Oracle's official `python-oracledb` 2.3 documentation states that the driver is tested with Python 3.7 through 3.12.
- OceanBase's official Python application guide documents Python 3.x access through `PyMySQL`, which is aligned with MySQL-compatible access patterns.
- The OceanBase OBCI documentation defines a C/OCI-compatible interface, with Linux, GCC, RPM, `libobclient`, and Oracle header prerequisites, and a prepare/bind/execute/fetch lifecycle identical to OCI.

Taken together, these sources suggest that a Python 3.7 implementation should avoid assuming a first-class Python Oracle-tenant driver on OceanBase. The safest first implementation is therefore a hybrid architecture: Python-native capture on Oracle, command-line replay against OceanBase Oracle mode, and an explicit future extension point for OBCI.

There is also an operational baseline already in use in `~/comparator`:

- Oracle connection config is stored as `[ORACLE_SOURCE] user/password/dsn`
- OceanBase config is stored as `[OCEANBASE_TARGET] executable/host/port/user_string/password`
- The OceanBase login identity is expressed as a full `obclient -u` value in `user_string`
- `obclient` passwords are passed through a temporary defaults file instead of appearing in process arguments

This existing contract should be preserved so operators can reuse established configuration habits and secret-handling expectations.

## Goals / Non-Goals

**Goals:**

- Deliver an implementation plan that is executable on Python 3.7 without changing the agreed product scope.
- Keep the full runtime distributable as one Python program for simpler production rollout and bug-fix replacement.
- Keep the staged JSONL pipeline from the original design so that capture, replay, and reporting remain independently rerunnable.
- Define a connector strategy that works in intranet environments and preserves a future path to native OBCI integration.
- Keep dependencies lightweight and installation-friendly for customer servers.

**Non-Goals:**

- Build a Python C extension or direct OBCI wrapper in the first implementation.
- Change the existing product goals, report outputs, or roadmap ordering.
- Deliver result-set verification, OCP integration, or PL/SQL profiler support in this change.

## Decisions

### 1. Keep the stage-decoupled Python architecture from the original design

The implementation will preserve the current three-stage structure inside one top-level Python program:

- `perf_comparator.py` will contain capture, replay, audit, and reporting functions or classes in one file
- stage boundaries remain JSONL artifacts and CLI subflows, not separate Python modules

Intermediate JSONL artifacts remain the contract between stages. This keeps reruns, offline debugging, and later connector changes low-risk.

Alternative considered:

- A single in-memory pipeline with no durable stage artifacts
- A multi-file package with separate scripts per stage

Rejected because the first option would make replay debugging, report-only reruns, and future connector swaps much harder, while the second conflicts with the production distribution requirement.

### 2. Use a single-file runtime layout

The full v1 implementation will live in one Python source file, expected to be `perf_comparator.py`. Internal concerns will still be separated using focused classes and helper functions, but they will not be split into multiple project modules.

Implementation direction:

- Keep parsing, config loading, capture adapters, replay adapters, audit polling, analysis, and rendering in one file
- Use sectioned classes and helper blocks to keep local cohesion
- Keep non-code assets external only when they are true runtime artifacts, such as generated JSONL, HTML reports, and SQL hint files

Alternative considered:

- A package layout with `sql_capture.py`, `sql_replay.py`, `sql_audit_daemon.py`, and `perf_report.py`

Rejected because frequent production fixes are operationally easier when rollout is a single Python file replacement.

### 3. Use `python-oracledb` 2.x for Oracle capture

Oracle capture depends on reading AWR, `V$SQL`, and related Oracle views. `python-oracledb` 2.3 is the best fit because Oracle's official documentation explicitly tests it with Python 3.7, and it already supports SQL, PL/SQL, bind variables, and connection pooling semantics needed for capture.

Implementation direction:

- Pin to a Python 3.7-compatible 2.x release in `requirements.txt`
- Reuse comparator's `[ORACLE_SOURCE]` contract with `user`, `password`, and `dsn`
- Keep `dsn` format as `host:port/service_name` to match existing operations practice
- Use standalone or pooled connections for capture
- Keep Oracle access isolated behind a capture adapter so the rest of the pipeline is not driver-specific

Alternative considered:

- Use `cx_Oracle`

Rejected because `python-oracledb` is the successor path and better aligned with current Oracle documentation while still supporting Python 3.7 in the 2.x line.

### 4. Use `obclient` subprocess execution as the baseline OceanBase Oracle-mode replay path

For OceanBase replay, the safest v1 baseline is to drive the Oracle tenant through `obclient` and SQL scripts from Python. This matches the original design's L0 capability assumption and avoids relying on an uncertified Python Oracle-mode driver path.

Implementation direction:

- Reuse comparator's `[OCEANBASE_TARGET]` contract with `executable`, `host`, `port`, `user_string`, and `password`
- Use a dedicated replay adapter that shells out to `obclient`
- Pass the OceanBase identity through `user_string` exactly as operators already provide it in comparator
- Keep passwords out of process arguments by writing them to a temporary defaults file or equivalent secure input path before invocation
- Set session parameters such as timeout before execution
- Run `EXPLAIN EXTENDED` and diagnostics queries through the same adapter
- Convert captured bind values into typed SQL literals for replay
- Mark statements as `skip` with explicit reasons when bind literalization is unsafe or unsupported

Alternative considered:

- Direct Python replay against OceanBase Oracle mode through a Python database driver

Rejected for v1 because the official OceanBase Python documentation is centered on `PyMySQL`, while Oracle-mode low-level compatibility is documented through OBCI, not a Python-native connector.

### 5. Keep OceanBase diagnostics in a separate daemon thread or subprocess controlled by the same program

The SQL Audit ring buffer constraint remains valid regardless of language. The audit collector will run independently from replay execution under the same top-level Python program, poll on a sub-500ms cadence, and append rows immediately to `audit_dump_<ts>.jsonl`.

Implementation direction:

- Separate control flow within `perf_comparator.py`
- Independent OceanBase session and high-water-mark polling by request ID
- Immediate append-only persistence to avoid in-memory loss

Alternative considered:

- Query SQL Audit only after each replayed statement

Rejected because buffer eviction under concurrent replay would make that approach unreliable.

### 6. Reserve OBCI for a later native bridge, not the initial Python implementation

The OBCI documentation is still valuable because it defines the long-term native path: install prerequisites, link model, OCI lifecycle, and trace metadata APIs. However, the operational cost is non-trivial: RPM deployment, GCC toolchain, `libobclient`, and Oracle headers.

To preserve this path without taking the deployment hit now, the replay layer will expose a backend interface:

- baseline backend: `obclient` script execution
- future backend: an OBCI-backed helper process or native bridge

The future OBCI path should reuse the same workload and replay artifact schema so report logic stays unchanged.

Alternative considered:

- Implement OBCI directly in Python first

Rejected because it introduces native build, packaging, and ABI complexity before the baseline product behavior is proven.

### 7. Prefer Python standard library components unless a dependency is clearly justified

The first implementation should rely on:

- `argparse` for CLI
- `configparser` for INI configuration, with comparator-compatible section names and keys
- `json` and append-only file I/O for JSONL
- `logging` for structured logs
- `subprocess` for `obclient` execution
- `threading` or `concurrent.futures` for replay and audit concurrency

Only database connectors should be external dependencies in v1 unless a later implementation step shows a real gap.

Alternative considered:

- Introduce a larger framework stack for CLI, templates, or orchestration

Rejected because it works against the deployment constraint and offers little value for the current scope.

## Risks / Trade-offs

- [Replay bind fidelity loss with `obclient`] -> Use type-aware literalization, mark unsupported cases explicitly, and keep a future OBCI bridge as the upgrade path for exact bind semantics.
- [`obclient` availability or version drift on customer servers] -> Add preflight checks, record capability artifacts, and fail early with actionable diagnostics.
- [Python 3.7 ecosystem drift] -> Pin dependency versions, prefer standard-library modules, and document offline wheel installation for intranet deployment.
- [Single-file program grows too large] -> Keep explicit section boundaries, narrow helper functions, and documented internal class ownership within the one-file constraint.
- [Configuration drift from comparator confuses operators] -> Reuse comparator's section names, key names, and DSN or `user_string` semantics unless a new field is strictly required.
- [OceanBase password leaks through command line or logs] -> Mirror comparator's secure invocation pattern by avoiding plain-text password arguments and redacting sensitive config values in logs.
- [SQL Audit data loss under load] -> Keep the independent daemon, poll every 300ms by default, and persist rows immediately to JSONL.
- [Future OBCI adoption requires native packaging] -> Keep OBCI behind a replay backend interface so the rest of the codebase is unaffected when the native path is introduced.

## Migration Plan

1. Establish the single-file Python 3.7 program skeleton, comparator-compatible config loading, requirements pinning, and JSONL utilities.
2. Implement Oracle capability probing and batch or stream workload capture with `python-oracledb`.
3. Implement the OceanBase replay adapter on top of `obclient`, plus replay capability probing and explain-plan capture.
4. Implement the SQL Audit daemon and correlation logic.
5. Implement report generation and the first rule set.
6. Add validation scripts and deployment documentation for intranet installation.

Rollback strategy:

- Because the project is not yet deployed, rollback is simply reverting the implementation branch or restoring the previous OpenSpec baseline.
- No existing runtime behavior is replaced by this change.

## Open Questions

- Is the target OceanBase tenant guaranteed to be Oracle mode, or must MySQL mode also be supported in v1?
- Can customer servers reliably provide `obclient` alongside the Python runtime, or must the tool ship its own client prerequisites checklist?
- Should the future OBCI path be a Python extension module or a standalone helper binary invoked by Python?
- What percentage of captured SQL is expected to require exact bind semantics that `obclient` literalization cannot safely represent?

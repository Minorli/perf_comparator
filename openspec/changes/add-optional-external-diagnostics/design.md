## Context

The current runtime already supports L0-L3 style replay diagnostics in one Python file, but two practical gaps remain.

1. `DBMS_PROFILER` collection assumes profiler system objects already exist. On fresh OB 4.2.5 tenants this is false, so profiling silently degrades or fails.
2. The design references an L4 layer with OCP and obdiag assistance, but the code has no configuration contract, capability signal, or collection path for either.

This change must preserve the current constraints:

- Python 3.7 only
- single-file runtime
- graceful degradation over hard failure
- no heavy SDKs or extra services

## Goals / Non-Goals

**Goals:**

- Make profiler collection self-bootstrapping and idempotent.
- Add lightweight, optional OCP and obdiag integration hooks without hard-coding a fragile vendor SDK dependency.
- Persist readiness and collection outcomes into existing capability or reporting artifacts.
- Add a root README that explains how to operate the tool in Oracle->OB and OB-only paths.

**Non-Goals:**

- Full OCP platform client implementation or tenant discovery workflow.
- Mandatory external diagnostics for every replay row.
- New artifact families beyond compact JSON sidecars embedded into existing capability and report outputs.

## Decisions

### 1. Bootstrap profiler objects lazily before the first profiled replay

The runtime will call `CALL DBMS_PROFILER.OB_INIT_OBJECTS(FALSE);` once per run when `plsql_profile=true` is needed. A cached in-memory status flag on `settings` avoids repeating the init for every SQL.

Why:

- keeps initialization local to the feature that needs it
- avoids extra startup cost for non-PLSQL runs
- supports idempotent repeated executions

Alternative considered:

- probing or initializing during `check-config` or replay capability probing
  Rejected because it turns an optional feature into a startup dependency and may require privileges operators do not want to test on every run.

### 2. Model OCP integration as URL-template fetches, not a hard-coded API client

Instead of binding to one OCP SDK or schema, config will accept optional URL templates for ASH and QPM style endpoints. The runtime will substitute placeholders such as `sql_id`, `run_id`, and `request_id`, fetch text or JSON with `urllib`, and persist a compact evidence record.

Why:

- remains Python 3.7 standard-library only
- avoids version lock to a specific OCP release
- lets intranet operators point to an API gateway, proxy, or wrapper service they already control

Alternative considered:

- direct vendor-specific OCP client
  Rejected because it would add dependency, auth, and compatibility burden that conflicts with the standalone intranet requirement.

### 3. Model obdiag integration as an optional subprocess collector

If an obdiag executable is configured, the runtime will invoke it only for severe regression rows or source-only hotspots, capture stdout or output directory references, and record status without failing the main workflow.

Why:

- matches the existing CLI-first architecture
- works in air-gapped environments
- keeps the integration narrow and auditable

Alternative considered:

- always run obdiag for every job
  Rejected because it is too expensive, noisy, and operationally risky.

### 4. Report external diagnostics as evidence, not as primary ranking signals

External diagnostics will appear in capability files, summary text, HTML evidence columns, and hints comments. They will not affect slowdown ranking or rule firing in this change.

Why:

- keeps existing analysis semantics stable
- makes the new behavior additive and easy to verify

## Risks / Trade-offs

- [OCP endpoint schema varies by deployment] -> Mitigation: use operator-supplied URL templates and accept either JSON or plain text responses.
- [obdiag command may be slow or unavailable] -> Mitigation: gate on explicit config and treat all failures as evidence-level warnings.
- [profiler init may require elevated privileges] -> Mitigation: cache failure reason and downgrade profiling cleanly instead of aborting replay.
- [README can drift from runtime behavior] -> Mitigation: keep it close to current modes and config fields already under test.

## Migration Plan

1. Add failing tests for profiler bootstrap, capability probing, external diagnostics evidence, and README presence.
2. Implement lazy profiler initialization and extend replay capability artifacts.
3. Add optional OCP fetch and obdiag subprocess collection helpers plus reporting hooks.
4. Update config template, runtime setup notes, and add root `README.md`.
5. Re-run unit tests, `py_compile`, and OpenSpec validation.

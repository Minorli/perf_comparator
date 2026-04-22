## Context

`perf_comparator` already has a useful OB source path:

- it captures `GV$OB_SQL_AUDIT` incrementally
- it backfills hidden SQL text when needed
- it emits a final source-only report

But the target operating model is closer to production observability:

- start once
- leave it running during a 24-hour test window
- inspect the report while the test window is still active
- tell each testing group which SQL or PL/SQL calls were slow and why

That requires two missing pieces:

1. caller attribution fields from source audit rows
2. rolling report regeneration on a configurable interval

## Goals / Non-Goals

**Goals**

- Capture caller attribution fields from `GV$OB_SQL_AUDIT`.
- Aggregate statements by dominant caller group.
- Generate rolling source-only report snapshots during capture.
- Surface separate slow SQL and slow PL/SQL sections with likely causes.

**Non-Goals**

- Full dashboard or web server
- Continuous OCP refresh on every rolling snapshot
- Perfect department inference when the source tenant does not expose stable caller identity

## Decisions

### 1. Caller attribution uses source audit fields and configurable identity composition

The source path will record these `GV$OB_SQL_AUDIT` fields when present:

- `TENANT_NAME`
- `DB_NAME`
- `USER_NAME`
- `USER_CLIENT_IP`
- `CLIENT_IP`
- `RET_CODE`

The runtime will build a caller-group key from configurable fields, defaulting to:

- `tenant_name`
- `db_name`
- `user_name`
- `user_client_ip`

Why:

- these fields are documented on OceanBase 4.2.x and are the most stable production attribution signals
- caller groups can approximate testing departments without needing application changes

### 2. Rolling source reports overwrite a stable run-scoped report path

During `source-report` capture, if `rolling_report_interval > 0`, the runtime will periodically regenerate the source report using the same `run_id`.

Why:

- operators can open the same HTML/TXT paths while the run is active
- no extra report-file families are needed

### 3. Rolling reports use a lighter diagnostics path than the final report

While capture is still running, rolling snapshots will skip expensive deep diagnostics such as source plan-monitor fetches and external-diagnostics expansion.

The final post-capture report will retain the richer existing behavior.

Why:

- a rolling snapshot must be cheap enough to run repeatedly for hours

### 4. Source-only reports separate SQL and PL/SQL hotspots

The source report will classify rows as `sql` or `plsql` and present:

- top slow SQL
- top slow PL/SQL
- top caller groups

Why:

- customers want a direct answer to "which SQL and which PL/SQL calls were slow"
- PL/SQL slow paths already have better diagnosis support than plain SQL and should be displayed distinctly

## Risks / Trade-offs

- [Caller groups may still be ambiguous] -> expose the exact composition fields used for attribution.
- [Rolling refresh may add load] -> make cadence configurable and skip deep diagnostics during live refresh.
- [Source report size may grow noisy] -> keep inline sections concise and rely on existing detailed row tables for depth.

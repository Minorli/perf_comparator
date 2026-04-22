## Context

There are two very different OceanBase-source use cases:

1. OceanBase -> OceanBase comparison, where the source workload is replayed elsewhere.
2. Single-OB troubleshooting, where no comparison target exists and the operator only wants to diagnose what is happening on that one cluster.

The current implementation only covers the first shape. This change adds the second one.

Constraints remain:

- Python 3.7
- single-file runtime
- no extra long-running services
- capability-gated behavior

## Goals / Non-Goals

**Goals**

- Support a mode that only requires `OCEANBASE_SOURCE`.
- Produce reports directly from source-side SQL Audit evidence.
- Preserve richer source metrics such as retry count, memstore vs ssstore reads, and bloom filtering.
- Keep the reporting experience aligned with the existing HTML/TXT/SQL-hint artifacts.

**Non-Goals**

- Replacing the migration-comparison pipeline.
- Adding real-time dashboards or background daemons beyond the existing polling loop.
- Requiring OCP.

## Decisions

### 1. Add an explicit `source-report` mode

This use case is operationally different enough from `batch` that it should be explicit in the CLI.

Implementation direction:

- `--mode source-report`
- require `source_db_mode=oceanbase`
- allow config with only `OCEANBASE_SOURCE`
- capture source workload for a bounded duration
- aggregate rows and emit the standard report artifact set

### 2. Build source-only report rows from SQL Audit aggregates

SQL Audit already provides most of the evidence needed for local troubleshooting.

Implementation direction:

- aggregate by `sql_id` and normalized SQL text
- preserve latest trace or request metadata for plan-monitor enrichment
- compute source-side average and total metrics
- reuse rule logic where it still makes sense without Oracle baseline

The real OB 4.2.5 validation showed that SQL Audit alone can miss some complex statements that are visible in plan cache.
The implementation therefore supplements SQL Audit with:

- a bounded `GV$OB_SQLSTAT` start/end snapshot delta for statements that executed during the capture window but did not appear in SQL Audit
- a `GV$OB_PLAN_CACHE_PLAN_STAT` recent-activity fallback keyed by `LAST_ACTIVE_TIME` for complex statements that still miss SQL Audit and SQLSTAT delta capture
- source-report filtering that removes the tool's own polling and backfill SQL from the final report ranking

### 3. Keep report language source-aware

When there is no Oracle baseline, fields like speedup ratio and plan-changed must degrade cleanly.

Implementation direction:

- display `source-only` mode in summaries
- rank by source elapsed time and frequency
- emphasize queue time, net time, retry count, plan cache misses, memstore pressure, and plan-monitor spill

## Risks / Trade-offs

- [Source SQL IDs may be short-lived or repeated across variants] -> aggregate by sql_id with SQL-text fallback.
- [Single-OB source tenants may not expose all GV$ views] -> capability-gate plan-monitor enrichment and degrade gracefully.
- [No Oracle baseline means fewer comparative signals] -> shift ranking to elapsed time, frequency, and bottleneck metrics.

## Migration Plan

1. Add new CLI mode and config validation rules.
2. Extend source SQL Audit capture schema.
3. Implement source-only aggregation and report generation.
4. Add tests for source-only config, aggregation, and CLI flow.
5. Run a real OB 4.2.5 source-only validation with complex SQL and package workload.

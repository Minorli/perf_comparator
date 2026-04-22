## Context

`perf_comparator` already supports native OCP SQL lookup and staged SQL text recovery, but two outputs are still thin:

1. native OCP evidence stops at SQL list and SQL text, without time-series trend context
2. source-only reporting does not summarize how often SQL text came from capture, source SYS lookup, native OCP, or template fallback

This change should remain additive and low risk:

- Python 3.7 only
- single-file runtime
- no new dependencies
- preserve current report fixture compatibility unless external diagnostics are explicitly enabled

## Goals / Non-Goals

**Goals:**

- Fetch OCP trend data when native OCP finds a SQL ID.
- Persist and surface resolved OCP cluster and tenant IDs even when they were derived from names.
- Add a compact SQL-source distribution view in source-only HTML and text outputs.

**Non-Goals:**

- Full OCP dashboard replication
- Additional long-running background polling
- New output file families beyond current reports and external evidence artifacts

## Decisions

### 1. Trend fetch is conditional on successful native SQL match

Only if native OCP already found a SQL ID will the runtime call the trends endpoint.

Why:

- avoids extra requests when there is no SQL correlation
- keeps latency bounded

### 2. Resolved OCP target identity is cached and reported

Resolved `cluster_id` and `tenant_id` will be cached in settings and copied into replay capability or row diagnostics summaries.

Why:

- operators need to verify which tenant the OCP evidence really came from
- avoids repeated name-resolution calls

### 3. SQL-source distribution uses existing row annotations

The source-only report will aggregate `source_sql_text_source` values already present on rows and render a lightweight SVG bar chart plus textual counts.

Why:

- reuses existing evidence instead of inventing new metadata
- keeps the report compact and browser-native

## Risks / Trade-offs

- [OCP trends endpoint schema may vary] -> Mitigation: parse defensively and persist raw JSON payloads.
- [More external requests can slow report-only mode] -> Mitigation: only fetch trends for rows with matched native OCP SQL IDs.
- [Additional source-report text may break fixtures] -> Mitigation: guard new text to source-only mode and adjust tests deliberately.

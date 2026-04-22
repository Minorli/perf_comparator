## Context

`perf_comparator` already collects profiler runs and hot-line timings, but its source lookup is still the first-cut implementation:

- it fetches `ALL_SOURCE` directly
- it treats source rows as line-based by default
- it does not distinguish exact line hits from reconstructed source lines

The user guidance and the existing `~/ProfilePKG` implementation both show that OceanBase source exposure is version-sensitive. In particular, package text may arrive either as normal line rows or as a single row containing LF-delimited source text. The reporting path therefore needs an explicit confidence model instead of silently presenting every hot line as exact.

Constraints remain unchanged:

- Python 3.7 compatible
- single-file runtime
- no extra Python dependencies
- profiler failures and source-mapping failures must degrade gracefully

## Goals / Non-Goals

**Goals**

- Detect OceanBase version for profiler evidence.
- Load source text adaptively from `DBA_SOURCE` and `ALL_SOURCE`.
- Reconstruct logical source lines when the source view returns a single LF-delimited row.
- Preserve blank lines and surrounding source context.
- Persist and report mapping strategy plus confidence.

**Non-Goals**

- Deploy helper packages or tables for source mapping in this change.
- Guarantee debugger-grade exactness for every OceanBase version.
- Extend profiler diagnostics to Oracle source mode.

## Decisions

### 1. Version detection is best-effort and cached

The runtime will call `SELECT OB_VERSION() FROM DUAL` once per run and cache the parsed version string.

Why:

- it is lightweight
- it already exists in adjacent OceanBase tooling
- reports need to show which version the mapping decision was based on

If the query fails, the profiler path continues with `ob_version = unknown`.

### 2. Source mapping uses adaptive view probing instead of a fixed join

The runtime will load source text through a helper that tries `DBA_SOURCE` first and falls back to `ALL_SOURCE`.

For each view it will:

- fetch `LINE` and `TEXT`
- preserve row order
- normalize CRLF to LF
- detect whether the returned rows are already line-based or whether a row contains embedded LF that must be split

Why:

- `DBA_SOURCE` is the most reliable source when privileges exist
- `ALL_SOURCE` still provides a fallback for lower-privilege environments
- shape detection is safer than hardcoding one layout per version

### 3. Mapping confidence is explicit

Each profiler hot line will carry:

- `source_mapping_strategy`
- `source_mapping_confidence`
- `source_view`
- `source_layout`
- `ob_version`
- `source_line_hit`

Confidence rules:

- `high`: source rows were line-based and the requested line matched directly
- `medium`: source text was reconstructed by splitting LF-delimited text and the requested line resolved
- `low`: source text was available but the requested line could not be resolved exactly
- `none`: source text could not be loaded

Why:

- operators need to distinguish exact line hits from reconstructed evidence
- reports must stop implying perfect precision when the mapping path is approximate

### 4. Reports summarize both hotspot and mapping quality

The reporting layer will continue to show the hotspot summary, but it will also include a compact mapping summary such as strategy plus confidence.

Why:

- profiler evidence is only actionable if the operator can trust the line mapping
- the existing summary/hints outputs already have a concise evidence section where this information fits naturally

## Risks / Trade-offs

- [Some tenants may not grant `DBA_SOURCE`] -> fall back to `ALL_SOURCE`, and report the actual source view used.
- [Large package source fetches may add latency] -> cache source lines per `(owner, name, type)` during the run.
- [Single-row CLOB splitting can still be imperfect across versions] -> mark reconstructed mappings as `medium` instead of `high`.
- [Report text changes can break fixtures] -> add explicit tests for mapping summary output.

## Migration Plan

1. Add OpenSpec deltas for replay diagnostics and reporting.
2. Add failing tests for version probing, source reconstruction, and mapping-summary reporting.
3. Implement adaptive source loading and cached OB version detection.
4. Persist mapping metadata into profiler artifacts and replay rows.
5. Update report outputs and run full validation.

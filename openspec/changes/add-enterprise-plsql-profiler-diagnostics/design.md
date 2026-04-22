## Context

The profiler path already captures line-level timing, version-aware source mapping, and a basic hotspot summary. That is enough for a proof of concept, but not for real package troubleshooting. A complex package often has:

- multiple nested procedures
- adjacent hot lines that belong to one logical slow block
- different classes of bottlenecks, such as row-by-row DML, dynamic SQL fan-out, commit storms, or pure CPU loops

Operators need the tool to compress profiler data into a clear diagnosis chain:

1. which unit dominates runtime
2. which contiguous block is hot
3. what anti-pattern that block resembles
4. which concrete rewrite direction is appropriate

Constraints remain unchanged:

- Python 3.7 compatible
- single-file runtime
- no extra dependencies
- graceful degradation when profiler metadata is incomplete

## Goals / Non-Goals

**Goals**

- Add unit-level profiler summaries.
- Group adjacent hot lines into hot blocks.
- Classify common PL/SQL slowdown patterns from source text and context.
- Surface those diagnoses in reports and hint output.

**Non-Goals**

- Full call-tree profiling
- Automatic package rewriting
- Replacing `DBMS_PROFILER` with another tracing subsystem

## Decisions

### 1. Add unit summaries from profiler tables

The runtime will query `PLSQL_PROFILER_UNITS` plus `PLSQL_PROFILER_DATA` for aggregated unit timing.

Each unit summary includes:

- owner, unit name, unit type
- total time
- total occurrences
- share of sampled profiler time

Why:

- the first question in enterprise debugging is usually "which procedure or package body dominates"

### 2. Group contiguous hot lines into hot blocks

Adjacent or near-adjacent hot lines within the same unit will be grouped into a hot block.

Each block includes:

- unit identity
- start line / end line
- total time and occurrence
- representative source lines
- block share of sampled profiler time

Why:

- a logical bottleneck is often a 3-10 line region, not a single line

### 3. Diagnose profiler patterns from source and context

The runtime will classify hot blocks using simple, deterministic heuristics:

- `row_by_row_sql_in_loop`
- `dynamic_sql_in_loop`
- `frequent_commit_in_loop`
- `tight_cpu_loop`
- `bulk_candidate`

Diagnosis is derived from:

- hotspot source line
- context lines
- occurrence count
- aggregated block shape

Why:

- this turns profiler output into a root-cause hypothesis that operators can act on immediately

### 4. Keep summary concise but persist rich evidence

Replay rows and profiler artifacts will store:

- `plsql_profile_unit_summary`
- `plsql_profile_hot_blocks`
- `plsql_profile_diagnoses`
- `plsql_profile_diagnosis_summary`

Reports will show a compact summary string, while artifacts retain the full block and unit details.

Why:

- terminal and HTML report surfaces must stay readable
- evidence artifacts should remain rich enough for escalation and support transfer

## Risks / Trade-offs

- [Pattern heuristics can overfit] -> keep them explicit, deterministic, and easy to inspect in tests.
- [More profiler SQL can add latency] -> only run extra aggregation when profiler mode is already enabled.
- [Too much report detail can become noisy] -> show only the top diagnosis summary inline and keep full evidence in JSONL.

## Migration Plan

1. Add OpenSpec deltas for richer profiler diagnostics.
2. Add tests with a complex synthetic package hotspot case.
3. Implement unit summary queries, hot-block grouping, and diagnosis heuristics.
4. Extend recommendations and reports to use diagnosis summaries.
5. Validate with py_compile, unit tests, and OpenSpec.

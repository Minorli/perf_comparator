## Context

The current `perf_comparator.py` already supports Oracle-source capture, OceanBase-source capture, replay, SQL Audit polling, and baseline reporting. What remains are the deeper evidence layers that make performance conclusions defensible in production review:

- result correctness checks for `SELECT` statements
- operator-level evidence for distributed slow SQL
- richer reports that can explain not only that a regression exists, but why it exists

These additions must preserve the current constraints:

- Python 3.7 compatible
- single-file runtime
- lightweight dependencies
- customer intranet deployability

## Goals / Non-Goals

**Goals:**

- Add optional result-set verification for `SELECT` statements.
- Add bounded `GV$SQL_PLAN_MONITOR` collection when replay evidence indicates distributed or slow execution.
- Make reports show verification status, mismatch samples, and operator-level evidence.
- Preserve current JSONL stage boundaries and the single-file deployment model.

**Non-Goals:**

- Full PL/SQL line profiler integration in this change.
- OCP integration in this change.
- Replacing the existing replay or report artifact formats wholesale.

## Decisions

### 1. Implement verification as an optional replay extension

Verification is expensive and not relevant to every run. It should therefore be optional and enabled through config or CLI.

Implementation direction:

- Only attempt verification for statements that look like `SELECT`
- Execute source and target queries separately
- Normalize rows into a stable textual representation
- Sort and hash the normalized rows
- If hashes differ, persist a bounded sample of mismatched rows

Alternative considered:

- Always verify every replayed statement

Rejected because it would add unnecessary runtime cost and does not apply to DML or PL/SQL blocks.

### 2. Query `GV$SQL_PLAN_MONITOR` only when it is likely to matter

Operator-level monitoring is most useful for distributed slow SQL. It should therefore be gated by replay evidence.

Implementation direction:

- Trigger plan monitor lookup when:
  - plan type is distributed, or
  - network ratio is high, or
  - speedup ratio is below the slowdown threshold
- Persist plan monitor rows into the replay record
- Parse enough fields to reason about row skew, memory usage, and spill

Alternative considered:

- Collect plan monitor rows for every statement

Rejected because it adds runtime cost without improving signal quality for simple local statements.

### 3. Extend reports with evidence, not just heuristics

The current report already includes heuristics and recommendations. It now needs to show stronger evidence to support those recommendations.

Implementation direction:

- Add verification status columns and mismatch references
- Add plan-monitor summaries for top regressions
- Extend recommendations with evidence-backed rule triggers

Alternative considered:

- Keep evidence only in JSONL and not surface it in reports

Rejected because operators need first-class visibility without opening raw artifacts manually.

## Risks / Trade-offs

- [Verification can be expensive on large result sets] -> Gate it with explicit opt-in and row limits.
- [Plan monitor may be unavailable in some environments] -> Treat it as capability-gated and degrade cleanly.
- [Single-file runtime grows further] -> Keep helpers grouped by feature and verify behavior through tests.

## Migration Plan

1. Add config and CLI controls for verification.
2. Implement verification helpers and mismatch artifacts.
3. Implement plan monitor collection and parsing.
4. Enrich reports with verification and monitor evidence.
5. Add tests and smoke validation.

## Open Questions

- Should verification default to off or on in production templates?
- What mismatch sample size is acceptable for intranet deployments with large result sets?

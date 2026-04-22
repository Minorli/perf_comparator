# perf_comparator Project Context

## Summary

`perf_comparator` is an Oracle to OceanBase SQL and PL/SQL performance comparison tool.
It captures Oracle workload evidence, replays representative SQL on OceanBase, correlates diagnostic telemetry, and produces actionable regression analysis for migration teams.

## Product Goals

- Identify SQL statements that are fast on Oracle but slow on OceanBase.
- Compare Oracle and OceanBase execution-plan behavior for each regression candidate.
- Produce actionable OceanBase tuning advice that operators can execute directly.
- Keep the tool standalone from `~/comparator` and suitable for customer intranet deployment.

## Non-Negotiable Constraints

- Python 3.7 compatibility is required.
- Avoid heavy runtime dependencies such as Kafka, Docker, or similar platform infrastructure.
- The tool must run without changing application code or customer network topology.
- Capability detection and graceful degradation are required because Oracle and OceanBase environments expose different levels of observability.

## Baseline Scope

The initial OpenSpec baseline captures the planned `v1.0` product surface:

- Single-entry CLI orchestration for batch, stream, replay-only, and report-only workflows
- Oracle workload capture with capability probing and JSONL persistence
- OceanBase replay with SQL Audit protection and replay telemetry collection
- Performance analysis reporting with regression ranking, plan comparison, and rule-based optimization guidance

## Deferred Roadmap

These items are known future changes and should be proposed through OpenSpec changes instead of being treated as current baseline behavior:

- `v1.1`: operator-level plan monitor analysis and PL/SQL profiler driven diagnostics
- `v1.2`: richer stream mode maturity and OCP integration
- `v2.0`: result-set verification and mismatch reporting

## Source Material

The current baseline is derived from:

- `docs/superpowers/specs/2026-04-22-perf-comparator-design.md`

That design document remains a reference artifact, while OpenSpec files become the operational source of truth for future planning and implementation.

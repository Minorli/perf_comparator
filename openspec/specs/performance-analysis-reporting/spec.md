# Performance Analysis Reporting Specification

## Purpose

Define how replay evidence is analyzed into migration-focused regression reports, plan-diff views, and actionable OceanBase optimization guidance.

## Requirements

### Requirement: Compute regression comparison signals
The reporting stage SHALL compute normalized comparison signals from Oracle and OceanBase evidence, including speed, plan change, read amplification, and network-cost indicators.

#### Scenario: Reporting processes a replayed SQL event
- GIVEN a replay record with Oracle and OceanBase timing and read metrics
- WHEN the reporting stage analyzes that record
- THEN it computes comparison signals such as speedup ratio, plan-changed status, read amplification, and network ratio
- AND uses those signals for ranking and diagnosis

### Requirement: Produce operator-focused report outputs
The reporting stage SHALL generate a browser-readable HTML report, a text summary, and an executable SQL hint artifact for each analysis run.

#### Scenario: Full report generation succeeds
- GIVEN a replay artifact that contains at least one analyzed SQL statement
- WHEN the reporting stage completes
- THEN it writes a timestamped HTML report, text summary, and SQL hint file
- AND the HTML report includes overview metrics, slow-query ranking, plan-change inspection, optimization guidance, and replay-error classification

#### Scenario: Operators need quick command-line review
- GIVEN a support engineer working in a terminal-only environment
- WHEN the reporting stage completes
- THEN the text summary provides a concise view of replay success, major regressions, and recommended next actions

### Requirement: Highlight plan-shape transitions across engines
The reporting stage SHALL compare Oracle and OceanBase plan shapes using an operator translation model so that risky migrations are surfaced explicitly.

#### Scenario: A local Oracle access path becomes a risky OceanBase path
- GIVEN a SQL statement whose Oracle plan uses an indexed row access path
- WHEN the OceanBase plan maps that access into a high-risk lookup or distributed path
- THEN the report highlights the translated operator difference
- AND marks the execution as a plan-change candidate for investigation

### Requirement: Emit rule-based optimization guidance
The reporting stage SHALL run extensible expert rules against replay evidence and produce diagnosis text together with actionable tuning guidance.

#### Scenario: Distributed join regression is detected
- GIVEN a replay record with distributed plan type and dominant network cost
- WHEN the expert rules are evaluated
- THEN the report emits a distributed-join diagnosis
- AND the SQL hint artifact includes concrete table-group or co-location guidance

#### Scenario: Plan cache miss regression is detected
- GIVEN a replay record with repeated plan misses and elevated plan-acquisition cost
- WHEN the expert rules are evaluated
- THEN the report emits a plan-cache diagnosis
- AND the recommendation includes statistics or plan-binding guidance that an operator can execute

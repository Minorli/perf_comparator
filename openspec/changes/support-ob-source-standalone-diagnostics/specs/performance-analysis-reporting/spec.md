# Delta for Performance Analysis Reporting

## ADDED Requirements

### Requirement: Reports SHALL support source-only OceanBase hotspot analysis
The reporting stage SHALL support source-only hotspot analysis when the input evidence comes from a single OceanBase source environment.

#### Scenario: Source-only report generation succeeds
- **WHEN** the reporting stage receives aggregated source-side SQL Audit evidence
- **THEN** it ranks problematic statements by source-side hotspot metrics
- **AND** emits HTML, text summary, and SQL-hint outputs without requiring Oracle comparison fields

### Requirement: Reports SHALL emphasize source-side bottleneck evidence
The reporting stage SHALL surface source-side bottlenecks such as queueing, distributed execution, plan cache miss, retry pressure, and LSM read pressure.

#### Scenario: Source statement has queueing and distributed cost
- **WHEN** a source-only statement shows high queue time, high network time, or distributed plan type
- **THEN** the report highlights those signals directly
- **AND** the recommendation text remains actionable for OceanBase operators

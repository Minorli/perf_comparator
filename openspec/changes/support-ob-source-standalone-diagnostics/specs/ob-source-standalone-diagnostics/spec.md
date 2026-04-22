# Delta for OB Source Standalone Diagnostics

## ADDED Requirements

### Requirement: Capture source-side SQL Audit evidence for standalone diagnosis
The system SHALL capture source-side SQL Audit rows from a single OceanBase environment and persist enough evidence for later troubleshooting.

#### Scenario: Source SQL Audit returns hotspot rows
- **WHEN** matching rows are observed in `GV$OB_SQL_AUDIT`
- **THEN** the system persists SQL text, timing breakdown, plan type, cache signals, retry count, and storage-layer read counters
- **AND** later reporting can aggregate those rows without replay

### Requirement: Aggregate source-side workload into operator-facing hotspots
The system SHALL aggregate source-side SQL Audit rows into operator-facing hotspot records suitable for reports.

#### Scenario: Same SQL appears many times in the capture window
- **WHEN** multiple SQL Audit rows map to the same SQL identity
- **THEN** the system aggregates occurrence count and average or total hotspot metrics
- **AND** the resulting report row preserves the strongest troubleshooting signals

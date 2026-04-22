# Delta for OB Replay Diagnostics

## ADDED Requirements

### Requirement: Optional PL/SQL profiler evidence for replayed package workloads
The replay stage SHALL support optional DBMS_PROFILER evidence collection for replayed PL/SQL and package executions.

#### Scenario: PL/SQL replay runs with profiler enabled
- **WHEN** profiler diagnostics are enabled and a replayed statement is identified as PL/SQL
- **THEN** the system starts and stops DBMS_PROFILER around that execution
- **AND** records the associated profiler run metadata and hot-line evidence

#### Scenario: Profiler privileges are missing
- **WHEN** profiler diagnostics are enabled but the target environment cannot start or query DBMS_PROFILER
- **THEN** the system records profiler collection as unavailable or skipped
- **AND** does not abort the replay of unrelated SQL statements

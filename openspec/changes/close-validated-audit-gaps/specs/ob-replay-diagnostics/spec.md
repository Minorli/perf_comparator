## ADDED Requirements

### Requirement: Persist profiler evidence for rule consumption
The replay stage SHALL persist profiler hotspot evidence in a structured form that downstream rules can consume directly.

#### Scenario: Profiled PL/SQL replay completes
- **GIVEN** profiler collection is enabled and a replayed PL/SQL statement returns profiler hot lines
- **WHEN** the replay record is written
- **THEN** the replay record includes the hottest source line and source-context evidence
- **AND** downstream reporting rules can use that evidence without re-querying profiler tables

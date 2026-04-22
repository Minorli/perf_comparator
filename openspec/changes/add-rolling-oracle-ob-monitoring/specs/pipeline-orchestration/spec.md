# Delta for Pipeline Orchestration

## ADDED Requirements

### Requirement: Stream mode SHALL support rolling Oracle-to-OceanBase monitoring
The command-line runtime SHALL allow `stream` mode to continuously capture Oracle workload, replay newly observed SQL or PL/SQL statements on OceanBase, and refresh the same report artifacts while the monitoring window is still active.

#### Scenario: Long-running Oracle monitoring stays active
- **GIVEN** an operator runs `stream` mode for a multi-hour monitoring window
- **WHEN** new Oracle SQL or PL/SQL statements are observed during polling
- **THEN** the system appends the new workload rows
- **AND** replays the newly observed statement fingerprints on OceanBase
- **AND** refreshes the run-scoped report artifacts without waiting for the end of the window

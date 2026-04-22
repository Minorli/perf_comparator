# Delta for Pipeline Orchestration

## ADDED Requirements

### Requirement: Source-only mode SHALL support rolling report refresh
The command-line runtime SHALL support regenerating source-only reports while source capture is still active.

#### Scenario: Long-running source-only capture is active
- **GIVEN** an operator runs `source-report` mode for a long capture window
- **WHEN** the rolling report interval elapses and workload rows already exist
- **THEN** the system regenerates the source-only report for the same run
- **AND** operators can inspect updated report files before capture completes

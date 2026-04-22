# Delta for Pipeline Orchestration

## MODIFIED Requirements

### Requirement: Support staged execution modes
The system SHALL provide a single CLI entrypoint that supports `batch`, `stream`, `replay-only`, `report-only`, and `verify-realdb` execution modes.

#### Scenario: Live verification mode runs safe environment checks
- **GIVEN** a valid OceanBase target and optional Oracle or OB-source reference configs
- **WHEN** an operator runs the CLI with `--mode verify-realdb`
- **THEN** the system performs live connectivity and smoke-validation steps
- **AND** writes a structured verification summary artifact with evidence paths and pass/fail status

## ADDED Requirements

### Requirement: OceanBase-source-only workflows SHALL not require Oracle config
The system SHALL allow OceanBase-source-only workflows to run without `[ORACLE_SOURCE]` when Oracle connectivity is not needed.

#### Scenario: Operator runs OB-source batch without Oracle connectivity
- **WHEN** `source_db_mode=oceanbase` and the workflow does not request Oracle-only behaviors
- **THEN** the system accepts a config containing only `OCEANBASE_SOURCE`, `OCEANBASE_TARGET`, and relevant settings
- **AND** proceeds with capture, replay, and reporting for OB-source workloads

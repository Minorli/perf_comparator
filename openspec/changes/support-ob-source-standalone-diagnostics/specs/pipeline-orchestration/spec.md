# Delta for Pipeline Orchestration

## MODIFIED Requirements

### Requirement: Support staged execution modes
The system SHALL provide a single CLI entrypoint that supports `batch`, `stream`, `replay-only`, `report-only`, and `source-report` execution modes.

#### Scenario: Source-report mode runs source-side troubleshooting
- **GIVEN** a config that points only to an OceanBase source environment
- **WHEN** an operator runs the CLI with `--mode source-report`
- **THEN** the system captures source-side workload evidence from that OceanBase environment
- **AND** generates report artifacts without replaying against another database

## ADDED Requirements

### Requirement: Source-report mode SHALL allow source-only OceanBase configs
The system SHALL allow `source-report` to run with only `OCEANBASE_SOURCE` when no comparison target is needed.

#### Scenario: Operator provides only OCEANBASE_SOURCE
- **WHEN** the operator runs `source-report` with `source_db_mode=oceanbase`
- **THEN** the system accepts a config that omits `ORACLE_SOURCE` and `OCEANBASE_TARGET`
- **AND** proceeds with source-side capture and reporting

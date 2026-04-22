## MODIFIED Requirements

### Requirement: Detect replay diagnostics capability levels
The system SHALL probe OceanBase diagnostics capability levels before replay and persist the result for later analysis, including optional profiler, OCP, and obdiag connectors when configured.

#### Scenario: SQL Audit is enabled
- **GIVEN** an OceanBase target with `ob_enable_sql_audit=ON`
- **WHEN** the replay stage probes available diagnostics
- **THEN** the system records that SQL Audit metrics are available
- **AND** enables telemetry collection paths that depend on SQL Audit data

#### Scenario: Optional external diagnostics are configured
- **GIVEN** an operator has configured profiler, OCP URL templates, or an obdiag executable
- **WHEN** diagnostics probing completes
- **THEN** the system records whether each optional connector is ready, unavailable, or misconfigured
- **AND** persists those readiness signals in the replay capability artifact without aborting the workflow

#### Scenario: Only base replay diagnostics are available
- **GIVEN** a target that supports execution and `EXPLAIN EXTENDED` but not advanced telemetry or external connectors
- **WHEN** diagnostics probing completes
- **THEN** the system continues in a lower capability mode
- **AND** still records wall time, execution outcome, and explain-plan data

## ADDED Requirements

### Requirement: Initialize profiler objects lazily and idempotently
The replay stage SHALL initialize profiler system objects on first use before starting `DBMS_PROFILER` collection.

#### Scenario: Profiler objects are not initialized yet
- **GIVEN** a replayed PL/SQL statement with profiler collection enabled
- **AND** the target tenant has not created profiler objects yet
- **WHEN** the runtime attempts profiler collection
- **THEN** it first executes `DBMS_PROFILER.OB_INIT_OBJECTS(FALSE)`
- **AND** continues profiling when initialization succeeds

#### Scenario: Profiler initialization is unavailable
- **GIVEN** profiler collection is requested
- **AND** initialization fails because the package or privilege is unavailable
- **WHEN** replay continues
- **THEN** the replay result records profiler collection as skipped or degraded with the failure reason
- **AND** the SQL replay itself does not fail only because profiler bootstrap failed

### Requirement: Collect optional external diagnostics non-blockingly
The replay and reporting stages SHALL support optional OCP and obdiag collection for severe regressions without making them mandatory.

#### Scenario: OCP fetch succeeds for a severe regression
- **GIVEN** a replay result that qualifies for deeper investigation
- **AND** OCP endpoint templates are configured
- **WHEN** the runtime fetches external diagnostics
- **THEN** it stores a compact evidence record containing the request URL context and response summary
- **AND** attaches that evidence to downstream reports

#### Scenario: obdiag collection fails or times out
- **GIVEN** obdiag collection is configured for deeper investigation
- **WHEN** the subprocess exits non-zero or times out
- **THEN** the workflow records the failure reason as diagnostic evidence
- **AND** still completes replay and reporting for the affected SQL

# OceanBase Replay Diagnostics Specification

## Purpose

Define how captured workload is replayed on OceanBase, how runtime telemetry is collected safely, and how replay evidence is persisted for downstream analysis.

## Requirements

### Requirement: Detect replay diagnostics capability levels
The system SHALL probe OceanBase diagnostics capability levels before replay and persist the result for later analysis.

#### Scenario: SQL Audit is enabled
- GIVEN an OceanBase target with `ob_enable_sql_audit=ON`
- WHEN the replay stage probes available diagnostics
- THEN the system records that SQL Audit metrics are available
- AND enables telemetry collection paths that depend on SQL Audit data

#### Scenario: Only base replay diagnostics are available
- GIVEN an OceanBase target that supports execution and `EXPLAIN EXTENDED` but not advanced telemetry
- WHEN diagnostics probing completes
- THEN the system continues in a lower capability mode
- AND still records wall time, execution outcome, and explain-plan data

### Requirement: Protect SQL Audit data during replay
The system SHALL run an audit-dump collector frequently enough to reduce the risk of losing replay telemetry from the `GV$OB_SQL_AUDIT` ring buffer.

#### Scenario: Replay is generating audit rows quickly
- GIVEN a replay session with SQL Audit enabled
- WHEN new rows appear in `GV$OB_SQL_AUDIT`
- THEN the collector polls incrementally using the latest request identifier
- AND appends the results to a persistent audit-dump artifact before the ring buffer can evict them

#### Scenario: No new audit rows are available
- GIVEN a polling interval where no replay telemetry has been written
- WHEN the collector query returns no rows
- THEN the collector preserves its last request identifier
- AND waits for the next poll interval without truncating prior output

### Requirement: Replay each workload item with Oracle-aware controls
The replay stage SHALL substitute bind values, derive OceanBase timeout settings from Oracle elapsed time and operator thresholds, execute the SQL, and correlate execution evidence with explain-plan and audit telemetry.

#### Scenario: Replay succeeds with correlated diagnostics
- GIVEN a workload event with SQL text, Oracle timing, and bind values
- WHEN the replay stage executes that event on OceanBase
- THEN the stage applies the configured timeout policy
- AND records execution timing, explain-plan data, and correlated audit metrics for that execution

#### Scenario: Distributed slow replay triggers deeper diagnostics
- GIVEN a replayed SQL statement whose plan type is distributed and whose speedup ratio falls below the configured threshold
- WHEN deeper diagnostics are available
- THEN the system attempts to fetch operator-level monitor data for that execution
- AND stores the additional evidence with the replay result

### Requirement: Persist replay records as structured evidence
The replay stage SHALL write one replay JSONL record per workload event, including status, timing breakdown, plan metadata, cache or RPC signals, read metrics, and derived regression indicators.

#### Scenario: Replay completes successfully
- GIVEN a successful replay execution
- WHEN the replay record is written
- THEN the record includes replay status, wall-clock timing, elapsed-time breakdown, plan type, plan hit signals, cache signals, read counters, and derived comparison metrics

#### Scenario: Replay fails or times out
- GIVEN a replay execution that errors or exceeds timeout
- WHEN the replay record is written
- THEN the record preserves the failure status and error code
- AND downstream reporting can classify the execution without replaying it again

# Delta for Performance Analysis Reporting

## ADDED Requirements

### Requirement: Replay reports SHALL separate slow SQL and slow PL/SQL
The replay reporting path SHALL distinguish between plain SQL and PL/SQL hotspots so operators can quickly identify which class of workload regressed during Oracle-to-OceanBase monitoring.

#### Scenario: Rolling replay includes SQL and PL/SQL
- **GIVEN** the replay artifact contains both SQL and PL/SQL statements
- **WHEN** the report is generated
- **THEN** the report includes separate slow SQL and slow PL/SQL sections
- **AND** both sections remain visible during rolling refresh and the final report

### Requirement: Source-only reporting SHALL warn clearly when QUERY_SQL visibility is at risk
The source-only reporting path SHALL surface a prominent warning when the configured OceanBase source login is not a SYS login and SQL text visibility may depend on privileged access or hidden parameters.

#### Scenario: Source capture uses a non-SYS login
- **GIVEN** source-only capture is configured with a non-SYS OceanBase source login
- **WHEN** the runtime starts or generates a report
- **THEN** the operator sees a prominent warning describing the `QUERY_SQL` visibility risk
- **AND** the warning explains to use `SYS`, configure `[OCEANBASE_SOURCE_SYS]`, or enable `_enable_sql_audit_query_sql=true` when needed

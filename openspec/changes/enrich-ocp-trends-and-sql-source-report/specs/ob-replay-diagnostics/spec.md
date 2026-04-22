## ADDED Requirements

### Requirement: Fetch native OCP SQL trends for matched SQL
The native OCP provider SHALL fetch time-series trend data for SQL IDs that were matched successfully.

#### Scenario: Native OCP matched a SQL ID
- **GIVEN** native OCP collection found a matching SQL ID for a replay row
- **WHEN** diagnostics collection continues
- **THEN** the system queries the native OCP SQL trends endpoint for that SQL ID
- **AND** stores the resulting trend payload as part of the external evidence

#### Scenario: Native OCP trend fetch fails
- **GIVEN** native OCP matched a SQL ID
- **WHEN** the trends endpoint errors or returns unusable data
- **THEN** the system records the trend failure in diagnostic evidence
- **AND** preserves the already collected SQL list and SQL text evidence

### Requirement: Persist resolved native OCP target identity
The native OCP provider SHALL record the resolved cluster and tenant IDs used for each request.

#### Scenario: Target IDs were resolved from names
- **GIVEN** the operator configured cluster and tenant names instead of IDs
- **WHEN** native OCP diagnostics run
- **THEN** the system records the resolved cluster and tenant IDs in diagnostic evidence
- **AND** downstream reports can show which OCP target was actually queried

## ADDED Requirements

### Requirement: Support native OCP SQL diagnostics
The replay diagnostics stage SHALL support a native OCP provider that uses official OCP SQL endpoints when native OCP configuration is present.

#### Scenario: Native OCP configuration is complete
- **GIVEN** an operator configured OCP base URL, authorization-header env, cluster ID, and tenant ID
- **WHEN** diagnostics collection is triggered for a replay row
- **THEN** the system uses official OCP SQL endpoints instead of only generic templates
- **AND** records the collection outcome without blocking replay

#### Scenario: Native OCP configuration is incomplete
- **GIVEN** only partial native OCP settings are present
- **WHEN** capability probing runs
- **THEN** the system reports native OCP as misconfigured or unavailable
- **AND** preserves generic template collection when configured

### Requirement: Correlate replay rows to OCP SQL records through official list endpoints
The native OCP provider SHALL search official OCP SQL list endpoints within a configurable time window and fetch full SQL text for matched SQL IDs.

#### Scenario: A matching SQL is found in OCP
- **GIVEN** a replay row with SQL text and execution timestamp
- **WHEN** the native OCP provider queries `topSql` or `slowSql`
- **THEN** it records the matched SQL ID and summary metrics
- **AND** fetches the full SQL text from the official SQL text endpoint

#### Scenario: No OCP match is found
- **GIVEN** native OCP collection is enabled
- **WHEN** the list endpoints return no SQL candidate for the replay row
- **THEN** the system records a no-match status
- **AND** continues report generation without treating that as a failure

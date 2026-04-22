## ADDED Requirements

### Requirement: Summarize SQL text recovery source distribution in source-only reports
The source-only reporting path SHALL summarize where SQL text came from across the analyzed workload.

#### Scenario: Source-only report includes multiple SQL recovery paths
- **GIVEN** a source-only reportable workload where some SQL text was captured directly, some was backfilled locally, and some came from OCP
- **WHEN** report generation completes
- **THEN** the summary and HTML outputs include counts by SQL text source
- **AND** the HTML report renders a lightweight chart for that distribution

#### Scenario: Only one SQL source path appears
- **GIVEN** all source-only SQL text came from one path
- **WHEN** report generation completes
- **THEN** the report still shows the source distribution explicitly
- **AND** operators can confirm that no fallback path was needed

### Requirement: Surface resolved OCP target identity in reports
The reporting stage SHALL show resolved native OCP cluster and tenant identity when external OCP evidence is present.

#### Scenario: Native OCP evidence exists
- **GIVEN** a row includes native OCP diagnostics
- **WHEN** summary, HTML, or hints outputs are generated
- **THEN** the report includes the resolved OCP cluster and tenant identifiers or names used for collection
- **AND** operators can distinguish which OCP tenant the evidence belongs to

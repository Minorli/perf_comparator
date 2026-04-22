## ADDED Requirements

### Requirement: Report native OCP SQL evidence explicitly
The reporting stage SHALL surface native OCP SQL evidence separately from generic external diagnostics when native OCP collection is configured.

#### Scenario: Native OCP evidence exists
- **GIVEN** a reportable replay row with native OCP evidence
- **WHEN** summary, HTML, and hints outputs are generated
- **THEN** the outputs include the selected OCP SQL ID or match status
- **AND** reference persisted raw payload files for operator follow-up

#### Scenario: Native OCP collection is enabled but not usable
- **GIVEN** native OCP settings are incomplete, unauthorized, or TLS-blocked
- **WHEN** report generation completes
- **THEN** the report includes a concise native OCP degradation note
- **AND** core replay analysis remains available

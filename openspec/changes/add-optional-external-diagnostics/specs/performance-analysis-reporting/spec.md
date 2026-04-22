## ADDED Requirements

### Requirement: Surface external diagnostics evidence in reports
The reporting stage SHALL include optional profiler bootstrap, OCP, and obdiag evidence in report outputs when those connectors are configured or attempted.

#### Scenario: External diagnostics are available
- **GIVEN** a replay or source-only analysis run with external diagnostics evidence
- **WHEN** the reporting stage generates HTML, summary, and hints outputs
- **THEN** the outputs include the connector status and a concise evidence summary
- **AND** operators can locate any referenced external artifact or response body from the report context

#### Scenario: External diagnostics are configured but unavailable
- **GIVEN** an operator enabled OCP or obdiag collection
- **WHEN** the connector fails, times out, or is misconfigured
- **THEN** the generated reports state that degradation explicitly
- **AND** keep the core SQL performance analysis intact

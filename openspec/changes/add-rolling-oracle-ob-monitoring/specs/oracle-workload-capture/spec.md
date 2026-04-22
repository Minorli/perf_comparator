# Delta for Oracle Workload Capture

## ADDED Requirements

### Requirement: Stream capture SHALL use a broader capture limit than report display
The Oracle capture path SHALL support a dedicated capture limit so long-running monitoring does not lose newly observed SQL or PL/SQL statements merely because the final report only displays a smaller Top N set.

#### Scenario: Monitoring captures beyond report Top N
- **GIVEN** a long-running Oracle monitoring session
- **WHEN** more statements are observed than the report will later display
- **THEN** the capture stage persists up to the configured capture limit
- **AND** the reporting stage still applies its own smaller Top N view independently

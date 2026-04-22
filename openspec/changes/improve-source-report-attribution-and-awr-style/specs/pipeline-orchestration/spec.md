# Delta for Pipeline Orchestration

## ADDED Requirements

### Requirement: Validation workflow SHALL capture external report-layout references
The validation workflow SHALL allow collecting external reference artifacts, such as Oracle AWR HTML reports, when the team is improving report presentation.

#### Scenario: Report UX is being upgraded
- **GIVEN** a change updates HTML report structure
- **WHEN** a real Oracle environment is available
- **THEN** the validation workflow may generate an Oracle AWR HTML report as a reference artifact
- **AND** the resulting implementation documents which structural elements were borrowed

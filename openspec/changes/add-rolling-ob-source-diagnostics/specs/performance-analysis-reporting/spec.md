# Delta for Performance Analysis Reporting

## ADDED Requirements

### Requirement: Source-only reports SHALL attribute slow workload to caller groups
The source-only reporting path SHALL summarize which caller groups triggered the slow SQL and PL/SQL workload.

#### Scenario: Multiple caller groups hit the same tenant
- **GIVEN** source audit rows contain caller attribution fields for multiple groups
- **WHEN** the report is generated
- **THEN** the report includes top caller groups by elapsed time or sample volume
- **AND** operators can see which slow statements each group contributed

### Requirement: Source-only reports SHALL separate slow SQL and slow PL/SQL
The source-only reporting path SHALL distinguish between plain SQL and PL/SQL workloads.

#### Scenario: Test traffic includes both SQL and PL/SQL
- **GIVEN** a source-only workload contains both plain SQL and PL/SQL statements
- **WHEN** the report is generated
- **THEN** the report includes separate slow SQL and slow PL/SQL sections
- **AND** likely causes remain visible for each section

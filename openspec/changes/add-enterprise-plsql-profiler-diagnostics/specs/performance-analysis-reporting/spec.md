# Delta for Performance Analysis Reporting

## ADDED Requirements

### Requirement: Reports SHALL summarize profiler diagnoses for complex packages
The reporting stage SHALL present profiler findings as concise diagnosis summaries instead of only raw hot lines.

#### Scenario: Complex package diagnosis exists
- **GIVEN** a replay row contains profiler hot blocks and diagnoses
- **WHEN** report generation completes
- **THEN** the summary, HTML, and hint outputs include the top profiler diagnosis
- **AND** operators can see the unit, line range, and diagnosed anti-pattern

### Requirement: Reports SHALL emit diagnosis-aware PL/SQL guidance
The reporting stage SHALL turn profiler diagnoses into targeted PL/SQL remediation guidance.

#### Scenario: Dynamic SQL in loop is detected
- **GIVEN** profiler evidence classifies a hot block as dynamic SQL inside a loop
- **WHEN** recommendations are generated
- **THEN** the report includes diagnosis-aware guidance for set-based rewrite, bulk binding, or SQL hoisting
- **AND** the hint output is more specific than a generic "review the package line" comment

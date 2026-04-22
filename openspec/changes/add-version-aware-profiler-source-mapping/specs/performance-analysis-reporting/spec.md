# Delta for Performance Analysis Reporting

## ADDED Requirements

### Requirement: Reports SHALL surface profiler mapping confidence
The reporting stage SHALL show how profiler hotspot source lines were mapped so that operators can judge evidence quality.

#### Scenario: Hotspot line was mapped exactly
- **GIVEN** a replay row includes profiler evidence with direct line-based source mapping
- **WHEN** report generation completes
- **THEN** the summary, HTML, and hint outputs include the profiler hotspot
- **AND** show that the mapping confidence is `high`

#### Scenario: Hotspot line was reconstructed from source text
- **GIVEN** a replay row includes profiler evidence reconstructed from LF-delimited source text
- **WHEN** report generation completes
- **THEN** the report includes the hotspot summary
- **AND** explicitly shows the reconstructed mapping strategy or lower confidence instead of implying an exact line match

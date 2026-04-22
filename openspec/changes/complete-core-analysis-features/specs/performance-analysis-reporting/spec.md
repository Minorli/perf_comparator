# Delta for Performance Analysis Reporting

## ADDED Requirements

### Requirement: Reports SHALL surface verification and plan-monitor evidence
The reporting stage SHALL surface result verification outcomes and plan-monitor summaries so top regressions include both heuristic and evidence-backed diagnosis.

#### Scenario: Verified regression is reported
- **WHEN** a replayed `SELECT` statement has a verification outcome
- **THEN** the report includes verification status
- **AND** mismatches or skips are visible in operator-facing outputs

#### Scenario: Plan monitor evidence exists
- **WHEN** a replayed statement has operator-level plan monitor evidence
- **THEN** the report includes a plan-monitor summary
- **AND** the recommendation output reflects monitor-derived findings such as skew or spill

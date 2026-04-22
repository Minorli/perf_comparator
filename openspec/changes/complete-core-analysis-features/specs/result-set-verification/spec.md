# Delta for Result Set Verification

## ADDED Requirements

### Requirement: Optional result-set verification for SELECT statements
The system SHALL support optional result-set verification for replayed `SELECT` statements so operators can distinguish performance regressions from correctness regressions.

#### Scenario: Matching source and target results
- **WHEN** verification is enabled for a replayed `SELECT` statement
- **THEN** the system compares normalized source and target row hashes
- **AND** records a successful verification outcome when the hashes match

#### Scenario: Mismatched source and target results
- **WHEN** verification is enabled and the normalized row hashes differ
- **THEN** the system records a mismatch outcome
- **AND** persists a bounded sample of mismatched rows for investigation

#### Scenario: Oversized result set is skipped
- **WHEN** verification is enabled but the result set exceeds the configured sampling threshold
- **THEN** the system records a skipped verification outcome
- **AND** does not attempt full-row verification

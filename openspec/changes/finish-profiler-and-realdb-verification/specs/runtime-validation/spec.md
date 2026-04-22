# Delta for Runtime Validation

## ADDED Requirements

### Requirement: Import reference connection info without committing secrets
The system SHALL support importing runtime connection sections from local reference configs so live validation can reuse known-good endpoints without copying secrets into the repository.

#### Scenario: Comparator reference configs exist locally
- **WHEN** the operator points validation at local reference configs
- **THEN** the system loads the required connection sections from those files at runtime
- **AND** does not persist secrets into project files automatically

### Requirement: Persist machine-readable live validation evidence
The system SHALL write a structured validation artifact describing which live steps passed, failed, or were skipped.

#### Scenario: Verification completes
- **WHEN** live verification finishes
- **THEN** the system writes a timestamped validation summary artifact
- **AND** the artifact includes executed steps, outcome, and generated evidence paths

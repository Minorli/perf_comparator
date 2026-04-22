## ADDED Requirements

### Requirement: Capture from Oracle audit and WCR fallbacks
The system SHALL capture Oracle workload from Unified Audit or WCR input when higher-priority telemetry sources are unavailable.

#### Scenario: Unified Audit is available but AWR and V$SQL are not usable
- **GIVEN** an Oracle environment where `UNIFIED_AUDIT_TRAIL` is readable
- **AND** AWR or `V$SQL` capture is unavailable or not selected
- **WHEN** the capture stage runs
- **THEN** the system captures SQL text and available metadata from Unified Audit
- **AND** writes normalized workload JSONL rows instead of failing the run

#### Scenario: Operator supplies a WCR file
- **GIVEN** an operator passes a WCR path
- **WHEN** the capture stage runs
- **THEN** the system parses SQL statements from that WCR input
- **AND** normalizes them into workload JSONL rows using the common schema

### Requirement: Honor fallback capture source priority
The system SHALL select the first usable capture source in priority order `awr -> vsql -> unified_audit -> wcr -> sql_file`.

#### Scenario: AWR and V$SQL are unavailable, Unified Audit is readable
- **WHEN** capability probing completes
- **THEN** the system selects Unified Audit as the active capture source
- **AND** records that decision in the capture capability artifact

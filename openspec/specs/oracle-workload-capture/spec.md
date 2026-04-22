# Oracle Workload Capture Specification

## Purpose

Define how the system discovers available Oracle telemetry sources, captures representative SQL workload, and normalizes that workload into reusable JSONL artifacts.

## Requirements

### Requirement: Detect capture sources in priority order
The system SHALL probe Oracle capture capabilities in priority order and gracefully degrade to lower-fidelity sources when higher-fidelity sources are unavailable.

#### Scenario: AWR is available
- GIVEN an Oracle environment with access to `DBA_HIST_SQLSTAT` and `DBA_HIST_SQL_PLAN`
- WHEN the capture stage probes available sources
- THEN the system selects AWR as the preferred source
- AND records that capability decision for downstream stages

#### Scenario: Manual SQL text is the only available source
- GIVEN an environment where AWR, dynamic views, and audit trails are unavailable
- WHEN the operator provides `--sql-file <path>`
- THEN the system accepts SQL text input as the fallback capture source
- AND continues the workflow without requiring Oracle diagnostic packs

### Requirement: Persist capture capability evidence
The system SHALL write a capture capability artifact that records which Oracle sources were available and which source was selected for the current run.

#### Scenario: Capability probing completes
- GIVEN a capture run in any mode
- WHEN source probing finishes
- THEN the system writes a timestamped capability artifact describing the detected Oracle telemetry sources
- AND downstream stages can use that artifact as context

### Requirement: Normalize workload into JSONL events
The system SHALL persist captured Oracle workload as JSONL where each event contains normalized SQL text, bind values when available, Oracle execution metrics, and Oracle plan information.

#### Scenario: Captured SQL includes bind and plan metadata
- GIVEN a SQL statement captured from a supported Oracle source
- WHEN the system writes the workload artifact
- THEN the event includes SQL identity, normalized SQL text, bind variables when available, execution counters, elapsed time metrics, logical or physical read metrics, and plan rows

#### Scenario: Capture source lacks some fields
- GIVEN a fallback capture source with incomplete telemetry
- WHEN the system writes the workload artifact
- THEN the event still preserves all available fields
- AND unavailable fields remain absent or null instead of blocking the run

### Requirement: Support incremental stream capture
The system SHALL support stream capture by polling `V$SQL` incrementally using a high-water mark and schema filters.

#### Scenario: Stream mode sees new SQL activity
- GIVEN a running stream-mode session with a previous `LAST_ACTIVE_TIME` watermark
- WHEN new matching SQL appears in `V$SQL`
- THEN the system appends only rows newer than the watermark to the workload artifact
- AND advances the watermark for the next poll

#### Scenario: Stream mode has no new data
- GIVEN a polling interval where no matching SQL has appeared
- WHEN the stream-mode query returns no new rows
- THEN the system leaves the workload artifact unchanged
- AND waits for the next polling interval without resetting state

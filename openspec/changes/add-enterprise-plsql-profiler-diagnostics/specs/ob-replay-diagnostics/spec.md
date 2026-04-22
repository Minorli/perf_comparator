# Delta for OB Replay Diagnostics

## ADDED Requirements

### Requirement: Profiler evidence SHALL aggregate complex package hotspots into diagnosable blocks
The replay stage SHALL aggregate profiler evidence into unit summaries and contiguous hot blocks so that complex package bottlenecks can be diagnosed reliably.

#### Scenario: Complex package has adjacent hot lines
- **GIVEN** profiler collection is enabled for a replayed package call
- **WHEN** multiple adjacent hot lines appear in the same package unit
- **THEN** the system groups them into a logical hot block
- **AND** records the block line range, total time, occurrence count, and representative source lines

#### Scenario: One unit dominates profiler time
- **GIVEN** profiler data spans multiple package units
- **WHEN** the profiler evidence is aggregated
- **THEN** the system records unit-level time summaries
- **AND** identifies which unit dominates sampled profiler time

### Requirement: Profiler evidence SHALL classify common PL/SQL slowdown patterns
The replay stage SHALL classify hot blocks into deterministic slowdown patterns when the source evidence supports it.

#### Scenario: Row-by-row SQL appears inside a loop
- **GIVEN** a hot block contains loop constructs together with DML or dynamic SQL
- **WHEN** profiler diagnoses are generated
- **THEN** the system records a row-by-row or dynamic-SQL-in-loop diagnosis
- **AND** downstream rules can reference that diagnosis directly

#### Scenario: Frequent commits appear inside a loop
- **GIVEN** a hot block contains commit statements with high occurrence counts
- **WHEN** profiler diagnoses are generated
- **THEN** the system records a frequent-commit-in-loop diagnosis
- **AND** the report can recommend batching or deferred commit strategies

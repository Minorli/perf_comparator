# Delta for OB Replay Diagnostics

## ADDED Requirements

### Requirement: Profiler evidence SHALL record version-aware source mapping metadata
The replay stage SHALL attach source-mapping metadata to PL/SQL profiler evidence so that operators can tell whether a hot line was resolved directly or reconstructed.

#### Scenario: Source rows map directly
- **GIVEN** profiler collection is enabled and the target source view exposes line-based source rows
- **WHEN** a PL/SQL profiler hot line is collected
- **THEN** the evidence records the OceanBase version, source view, and a `high` confidence direct line mapping
- **AND** the hot line keeps its matched source text and surrounding context

#### Scenario: Source text must be reconstructed
- **GIVEN** profiler collection is enabled and the target source view exposes source text as a single LF-delimited row
- **WHEN** a PL/SQL profiler hot line is collected
- **THEN** the system reconstructs logical source lines by splitting LF-delimited text
- **AND** records a non-`high` mapping confidence together with the strategy used

#### Scenario: Dictionary privileges are limited
- **GIVEN** profiler collection is enabled but `DBA_SOURCE` cannot be read
- **WHEN** the profiler path loads source text
- **THEN** the system falls back to `ALL_SOURCE` if available
- **AND** records which source view was ultimately used

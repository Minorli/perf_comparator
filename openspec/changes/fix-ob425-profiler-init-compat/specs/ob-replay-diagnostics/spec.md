# Delta for OB Replay Diagnostics

## ADDED Requirements

### Requirement: Profiler procedure wrappers SHALL be compatible with OceanBase 4.2.5
The replay stage SHALL execute profiler initialization and profiler start/stop calls using SQL wrappers that are valid on OceanBase 4.2.5.

#### Scenario: Profiler objects are initialized on OB 4.2.5
- **GIVEN** a target OceanBase 4.2.5 environment with `DBMS_PROFILER` installed
- **WHEN** profiler initialization runs
- **THEN** the runtime uses a valid wrapper form for `DBMS_PROFILER.OB_INIT_OBJECTS(FALSE)`
- **AND** profiler collection proceeds instead of being skipped for syntax reasons

#### Scenario: Profiler sampling starts and stops on OB 4.2.5
- **GIVEN** profiler collection is enabled for a replayed PL/SQL workload
- **WHEN** the runtime emits profiler start and stop SQL
- **THEN** it uses a wrapper form that is valid on OB 4.2.5
- **AND** the profiler smoke can produce real evidence artifacts

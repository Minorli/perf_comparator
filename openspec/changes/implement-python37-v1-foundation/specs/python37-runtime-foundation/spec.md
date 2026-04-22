# Delta for Python 3.7 Runtime Foundation

## ADDED Requirements

### Requirement: Python 3.7-compatible implementation baseline
The system SHALL implement the initial `perf_comparator` runtime with Python 3.7-compatible dependencies and standard-library features so it can be deployed on the current server baseline without requiring a Python upgrade.

#### Scenario: Python 3.7 deployment is prepared
- **GIVEN** a server that provides Python 3.7
- **WHEN** an operator installs the project's pinned dependencies and runtime prerequisites
- **THEN** the application runtime is installable without requiring Python 3.8 or later
- **AND** the deployment guide identifies any non-Python prerequisites explicitly

#### Scenario: Unsupported dependency is evaluated
- **GIVEN** a proposed dependency that does not support Python 3.7
- **WHEN** the implementation baseline is reviewed
- **THEN** that dependency is excluded from the default runtime path
- **AND** the design preserves a compatible alternative for the same responsibility

### Requirement: Single-file deployment unit
The system SHALL deliver all runtime functionality as one Python program so production rollout and urgent bug-fix replacement can be performed by distributing a single file.

#### Scenario: Production rollout is prepared
- **GIVEN** an operator is preparing to deploy or replace the tool on a production server
- **WHEN** the implementation artifacts are assembled
- **THEN** the executable runtime logic is contained in one Python source file
- **AND** deployment does not require synchronizing multiple application modules

#### Scenario: Internal concerns still remain separated
- **GIVEN** the single-file deployment requirement
- **WHEN** capture, replay, audit, and report logic are implemented
- **THEN** those concerns remain separated by functions, classes, or sections within the same file
- **AND** the stage artifact contracts remain unchanged

### Requirement: Connector roles are separated by runtime adapter boundaries
The system SHALL separate Oracle source access, OceanBase target execution, and artifact persistence behind runtime adapter boundaries so connector choices can vary without changing stage contracts.

#### Scenario: Oracle capture uses a Python-native driver
- **GIVEN** the capture stage is reading Oracle workload data
- **WHEN** the stage connects to Oracle
- **THEN** it uses the Oracle-side adapter selected for Python 3.7 compatibility
- **AND** the resulting workload artifact follows the same JSONL contract expected by downstream stages

#### Scenario: OceanBase Oracle-mode replay lacks a native Python adapter
- **GIVEN** the replay stage targets an OceanBase Oracle tenant
- **WHEN** no approved Python-native replay adapter is configured
- **THEN** the stage uses the baseline replay adapter defined for v1
- **AND** downstream replay and report artifacts remain unchanged

### Requirement: Connection configuration SHALL align with comparator conventions
The system SHALL reuse the established `~/comparator` connection configuration contract so operators can supply credentials using familiar section names and field semantics.

#### Scenario: Oracle source configuration is provided
- **GIVEN** an operator is preparing the tool configuration
- **WHEN** the Oracle source connection is defined
- **THEN** the configuration uses `[ORACLE_SOURCE]` with `user`, `password`, and `dsn`
- **AND** the `dsn` format remains `host:port/service_name`

#### Scenario: OceanBase target configuration is provided
- **GIVEN** an operator is preparing the OceanBase target connection
- **WHEN** the target connection is defined
- **THEN** the configuration uses `[OCEANBASE_TARGET]` with `executable`, `host`, `port`, `user_string`, and `password`
- **AND** `user_string` carries the full `obclient -u` identity value

#### Scenario: OceanBase source mode is enabled
- **GIVEN** an operator wants to capture workload directly from OceanBase
- **WHEN** `[SETTINGS].source_db_mode` is set to `oceanbase`
- **THEN** the configuration also requires `[OCEANBASE_SOURCE]` with `executable`, `host`, `port`, `user_string`, and `password`
- **AND** the runtime uses that section for source-side workload capture

#### Scenario: OceanBase password is passed to obclient
- **GIVEN** the replay stage is about to invoke `obclient`
- **WHEN** the target password is supplied to the client process
- **THEN** the password is not exposed as a plain-text process argument
- **AND** the runtime uses a safer input path such as a temporary defaults file or equivalent protected mechanism

### Requirement: OceanBase replay preserves a future OBCI integration seam
The system SHALL keep OceanBase Oracle-mode replay behind a backend abstraction so a future OBCI-backed implementation can be introduced without changing workload, replay, or report schemas.

#### Scenario: OBCI is unavailable in v1 deployment
- **GIVEN** a deployment environment without OBCI packages or native build prerequisites
- **WHEN** the replay stage is configured
- **THEN** the baseline replay backend remains usable
- **AND** the absence of OBCI does not block v1 execution

#### Scenario: OBCI-backed replay is introduced later
- **GIVEN** a future deployment that provides the required OBCI runtime and helper integration
- **WHEN** the replay backend is switched from the baseline path to the OBCI-backed path
- **THEN** the upstream workload artifact format remains valid
- **AND** the downstream reporting pipeline continues to consume the same replay schema

### Requirement: OceanBase source workload capture SHALL support long-running observation windows
The system SHALL support capturing workload directly from an OceanBase source tenant over a configured duration so OB-first business and package performance testing can be recorded and analyzed.

#### Scenario: 24-hour OceanBase source capture is requested
- **GIVEN** `[SETTINGS].source_db_mode` is `oceanbase`
- **AND** `[OCEANBASE_SOURCE]` is configured
- **WHEN** an operator starts a 24-hour capture window
- **THEN** the runtime polls `GV$OB_SQL_AUDIT` incrementally during that window
- **AND** every observed SQL execution is appended to the workload artifact

#### Scenario: OceanBase source metrics feed later comparison
- **GIVEN** workload was captured from an OceanBase source tenant
- **WHEN** replay and reporting stages process that workload
- **THEN** source-side elapsed and read metrics are preserved as the baseline for comparison
- **AND** downstream analysis does not require a separate schema format

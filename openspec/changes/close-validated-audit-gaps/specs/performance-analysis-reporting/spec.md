## ADDED Requirements

### Requirement: Emit executable tuning templates for high-value rules
The reporting stage SHALL emit executable or directly adaptable templates for the highest-value tuning rules instead of comment-only placeholders.

#### Scenario: Distributed join regression is detected
- **GIVEN** a row that triggers `DIST-JOIN`
- **WHEN** the reporting stage writes `perf_hints_<ts>.sql`
- **THEN** the artifact includes tablegroup DDL templates or equivalent executable tuning scaffolding

#### Scenario: Plan cache miss regression is detected
- **GIVEN** a row that triggers `PLAN-MISS`
- **WHEN** the reporting stage writes `perf_hints_<ts>.sql`
- **THEN** the artifact includes executable or directly adaptable statistics and plan-binding templates

### Requirement: Tie PLSQL-RPC recommendations to profiler evidence
The reporting stage SHALL use profiler hotspot evidence when generating `PLSQL-RPC` recommendations.

#### Scenario: Profiler confirms a hot PL/SQL loop
- **GIVEN** a row that triggers `PLSQL-RPC`
- **AND** profiler hot-line evidence is present
- **WHEN** the reporting stage generates recommendations
- **THEN** the recommendation text cites the hottest unit or line
- **AND** the hint artifact includes rewrite templates grounded in that hotspot context

### Requirement: Render overview charts in HTML reports
The reporting stage SHALL render lightweight overview charts in the HTML report.

#### Scenario: Report generation succeeds
- **GIVEN** a reportable analysis run
- **WHEN** the HTML report is generated
- **THEN** the report includes a regression-distribution chart
- **AND** a timing-comparison chart for the selected result set

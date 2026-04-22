# Delta for Performance Analysis Reporting

## ADDED Requirements

### Requirement: Reports SHALL translate risky operator transitions
The reporting stage SHALL express plan changes as translated Oracle -> OceanBase operator-risk signals instead of only plan-hash differences.

#### Scenario: Oracle indexed row access becomes risky lookup
- **WHEN** an Oracle indexed row-access pattern maps to a risky OceanBase lookup or distributed path
- **THEN** the report highlights the translated operator risk
- **AND** includes an action-oriented recommendation

### Requirement: Reports SHALL surface profiler-backed PL/SQL hotspots
The reporting stage SHALL surface profiler-backed hot lines for replayed PL/SQL regressions when profiler evidence exists.

#### Scenario: Package replay has profiler evidence
- **WHEN** a replayed PL/SQL statement has profiler hot-line evidence
- **THEN** the report includes the top hot lines and source text summary
- **AND** related rules can reference that evidence directly

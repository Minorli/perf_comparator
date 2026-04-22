# Delta for OB Replay Diagnostics

## ADDED Requirements

### Requirement: Plan monitor evidence for distributed or slow SQL
The replay stage SHALL collect `GV$SQL_PLAN_MONITOR` evidence for statements whose replay evidence indicates distributed or materially slow execution.

#### Scenario: Distributed slow SQL is replayed
- **WHEN** a replayed statement has distributed plan type or exceeds slowdown thresholds
- **THEN** the system queries `GV$SQL_PLAN_MONITOR`
- **AND** stores structured operator-level evidence with the replay result

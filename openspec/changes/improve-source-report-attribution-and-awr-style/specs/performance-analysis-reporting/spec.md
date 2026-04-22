# Delta for Performance Analysis Reporting

## ADDED Requirements

### Requirement: Source-only caller attribution SHALL prioritize audit-backed identity
The source-only reporting path SHALL prioritize caller attribution taken directly from audit rows over fallback identities derived from schema or SQL-only metadata.

#### Scenario: Mixed direct and fallback attribution exists for the same SQL ID
- **GIVEN** a source-only workload contains both audit-backed rows and fallback-only rows for the same SQL ID
- **WHEN** the report computes top caller groups and primary actor
- **THEN** it uses the audit-backed attribution for ranking and labeling
- **AND** it only falls back when no direct attribution exists

### Requirement: HTML reports SHALL support SQL-centric drill-down
The reporting path SHALL present SQL findings with navigable anchors and section-level summaries so operators can move from top lists to detailed evidence quickly.

#### Scenario: Operator reviews a top SQL item in HTML
- **GIVEN** the HTML report contains a top SQL or top PL/SQL section
- **WHEN** the operator clicks a SQL ID or summary link
- **THEN** the report jumps to the detailed finding for that SQL ID
- **AND** the detailed section includes SQL ID, SQL text, evidence, cause, and related metrics

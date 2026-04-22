# Oracle AWR HTML Reference Notes

## Reference Artifact

- Source file: `/tmp/oracle_awr_reference.html`
- Generated on: `2026-04-22`
- Database ID: `1272353027`
- Instance: `1`
- Snapshot range: `3266-3267`

## Observed AWR Structure

The Oracle HTML AWR report uses a stable operator-facing layout:

1. Top-level summary section near the beginning of the page
2. A compact navigation area made of in-page anchors
3. High-value sections ordered before long detail tables
4. SQL-centric sections grouped by elapsed time, CPU time, reads, gets, and executions
5. A complete SQL text section separated from the summary tables
6. Repeated "Back to Top" style navigation to reduce scrolling cost

Key section titles observed in the reference HTML:

- `Report Summary`
- `Time Model Statistics`
- `Foreground Wait Events`
- `SQL ordered by Elapsed Time`
- `SQL ordered by CPU Time`
- `SQL ordered by User I/O Wait Time`
- `SQL ordered by Gets`
- `SQL ordered by Reads`
- `SQL ordered by Executions`
- `Complete List of SQL Text`
- `ASH Report`

## Elements Borrowed Into perf_comparator

The comparator does not try to clone Oracle AWR wholesale. It borrows the parts that materially improve troubleshooting:

1. A fixed summary navigation block at the top of the HTML report
2. SQL ID links in top sections that jump to detailed findings
3. A dedicated `Detailed Findings` area with one anchor per SQL ID
4. Folded SQL text blocks so the report stays readable when SQL text is long
5. Stable section naming suitable for screenshots, customer handoff, and runbook references

## Source-Only Reporting Implications

For OceanBase source-only monitoring, the AWR-style layout is combined with attribution quality:

- `direct`: caller identity came from audit-backed attribution
- `fallback`: caller identity came from schema or SQL-only fallback
- `mixed`: both existed, but ranking prefers direct attribution

This prevents plan-cache-only rows from polluting top caller analysis while still preserving long-tail hotspot evidence.

## Replay Reporting Implications

For Oracle to OceanBase replay reports, the same drill-down structure is used so operators can:

1. Start from top SQL or top PL/SQL summaries
2. Click into the SQL ID detail card
3. Inspect SQL text, evidence, rules, monitor signals, and timing in one place

## Deliberate Non-Copy Choices

The comparator intentionally does not copy several Oracle AWR sections:

- wait-class matrices not backed by equivalent OB evidence in the current toolchain
- huge monolithic SQL text dumps without drill-down structure
- Oracle-specific instance statistics that do not map cleanly to the replay/source-only workflows

The objective is operator usefulness, not visual parity.

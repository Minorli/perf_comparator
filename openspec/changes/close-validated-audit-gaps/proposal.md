## Why

External audit triage found four implementation gaps that are real product deficits rather than roadmap misunderstandings:

- Oracle Unified Audit and WCR are probed but not actually capturable.
- High-value rules such as `DIST-JOIN`, `PLAN-MISS`, and `PLSQL-RPC` still emit descriptive comments instead of executable or directly adaptable templates.
- `PLSQL-RPC` does not consume profiler hotspot evidence, so the diagnosis stops at "likely loop" instead of surfacing the exact hot line.
- HTML reports remain table-only and do not include the charted overview promised by the design.

These gaps weaken the tool in exactly the places users feel most:

- source capture fallback when AWR/V$SQL are unavailable,
- copy-paste-able tuning output,
- causal PL/SQL evidence instead of generic suspicion,
- faster visual triage in the browser report.

## What Changes

- Implement Oracle capture fallbacks for Unified Audit and WCR input.
- Upgrade rule outputs so `perf_hints_<ts>.sql` contains executable DDL/SQL templates or structured code skeletons instead of comment-only placeholders.
- Feed profiler hotspot lines into `PLSQL-RPC` diagnosis and hint generation.
- Add lightweight browser-native charts to the HTML report without adding heavy frontend dependencies.

## Capabilities

### Modified Capabilities

- `oracle-workload-capture`: capture from Unified Audit and WCR when present.
- `ob-replay-diagnostics`: attach profiler evidence in a form that downstream rules can consume.
- `performance-analysis-reporting`: emit executable templates and charted overview sections.

## Impact

- Improves fallback capture coverage in restricted Oracle environments.
- Makes report outputs more actionable for DBAs and migration engineers.
- Strengthens the PL/SQL diagnosis chain from symptom to exact source hotspot.
- Preserves Python 3.7 and single-file runtime constraints.

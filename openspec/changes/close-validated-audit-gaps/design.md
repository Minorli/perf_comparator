## Context

The current runtime already has the major pipeline in place, so this change should avoid broad refactors.
The missing work is concentrated in four areas:

1. Oracle capture source selection stops at `awr`, `vsql`, and `sql_file`.
2. The recommendation engine writes human-readable comments, not directly usable operations.
3. Profiler evidence is collected, but `PLSQL-RPC` does not incorporate it.
4. HTML reports are static tables with no chart layer.

Constraints remain:

- single-file Python 3.7 runtime
- no heavy UI or plotting dependency
- graceful degradation when capture or profiler capabilities are missing

## Goals / Non-Goals

**Goals**

- Let Oracle capture runs actually use Unified Audit or WCR when available.
- Produce executable or directly adaptable artifacts for the highest-value recommendation classes.
- Enrich `PLSQL-RPC` with profiler hotspot evidence and line-aware code templates.
- Add compact visual summaries to HTML reports using inline SVG or minimal inline JavaScript.

**Non-Goals**

- OCP or obdiag integration in this change.
- Replacing the report layout wholesale.
- Building a full SQL parser or AST-based rewrite engine.

## Decisions

### 1. Implement Oracle fallback capture sources as first-class capture readers

Capability probing already distinguishes `unified_audit` and `wcr`, so the runtime should stop treating them as dead metadata.

Implementation direction:

- add a Unified Audit capture query that reads SQL text, schema, timestamp, and whatever execution metadata is available
- add a WCR reader that accepts a path from config or CLI and parses SQL statements into workload rows
- update capture-source priority to `awr -> vsql -> unified_audit -> wcr -> sql_file`
- keep all sources normalized into the same workload JSONL schema

### 2. Emit executable templates rather than comment-only hints

The tool cannot infer every object name safely, but it can emit templates that are runnable after minimal substitution.

Implementation direction:

- `DIST-JOIN`: emit tablegroup DDL template with named placeholders derived from available table names or aliases when present
- `PLAN-MISS`: emit statistics collection plus outline or binding skeletons
- `PLSQL-RPC`: emit `FORALL` and `MERGE` rewrite templates that include the detected unit, source line, and context when profiler data exists
- keep unsupported placeholders explicit so operators know what to fill in

### 3. Link profiler hotspots into `PLSQL-RPC`

Profiler should not remain a sidecar recommendation.

Implementation direction:

- when `PLSQL-RPC` preconditions match, inspect `plsql_profile_top_lines`
- surface the hottest line and source context directly inside the recommendation message
- generate templates that reference the profiled unit and line range
- retain the existing `PLSQL-HOTLINE` recommendation as corroborating evidence, not the only profiler-aware rule

### 4. Add lightweight report charts using inline SVG

The report only needs a compact visual overview, not a front-end stack.

Implementation direction:

- render an overview distribution chart for result buckets such as accelerated, neutral, mild regression, severe regression
- render a simple Oracle-vs-OB timing comparison chart for top selected rows
- keep charts self-contained in the generated HTML
- preserve terminal summary output without charts

## Risks / Trade-offs

- Unified Audit and WCR fields are poorer than AWR/V$SQL, so normalization must tolerate many nulls.
- Executable hint generation may still require placeholders where the runtime cannot infer object names safely.
- Profiler linkage can only be as precise as the profiled source line and context rows allow.
- Chart rendering must stay simple to avoid introducing browser or dependency fragility.

## Migration Plan

1. Add Oracle capture tests for Unified Audit and WCR.
2. Add recommendation tests for executable template output and profiler-linked `PLSQL-RPC`.
3. Add HTML report tests for chart sections.
4. Implement the runtime changes in the single-file engine.
5. Re-run OpenSpec validation and the Python test suite.

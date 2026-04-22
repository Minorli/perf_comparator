## 1. Oracle Capture

- [x] 1.1 Implement Unified Audit workload capture and normalize it into workload JSONL rows.
- [x] 1.2 Implement WCR file parsing and normalization into workload JSONL rows.
- [x] 1.3 Add tests covering capture-source priority and fallback to Unified Audit or WCR.

## 2. Executable Hints

- [x] 2.1 Upgrade `DIST-JOIN` hints to emit executable tablegroup DDL templates.
- [x] 2.2 Upgrade `PLAN-MISS` hints to emit executable or directly adaptable statistics and binding templates.
- [x] 2.3 Upgrade `PLSQL-RPC` hints to emit rewrite code templates instead of comment-only guidance.
- [x] 2.4 Add tests covering executable template output in `perf_hints_<ts>.sql`.

## 3. Profiler Linkage

- [x] 3.1 Feed profiler hotspot evidence into `PLSQL-RPC` rule evaluation and recommendation text.
- [x] 3.2 Include profiler line and source-context evidence in generated hints when available.
- [x] 3.3 Add tests covering profiler-linked `PLSQL-RPC` diagnosis.

## 4. HTML Visualization

- [x] 4.1 Add report overview charts for distribution buckets and Oracle-vs-OB timing comparison.
- [x] 4.2 Keep generated HTML self-contained using inline SVG or minimal inline JavaScript.
- [x] 4.3 Add tests covering chart sections in generated HTML.

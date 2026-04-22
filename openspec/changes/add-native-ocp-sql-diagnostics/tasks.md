## 1. Native OCP Config

- [x] 1.1 Add tests for native OCP config parsing and capability probing.
- [x] 1.2 Implement native OCP config fields and capability detection.

## 2. Native OCP Collection

- [x] 2.1 Add tests for native OCP SQL list and SQL text collection using mocked OCP responses.
- [x] 2.2 Implement native OCP request helpers, SQL matching, TLS handling, and artifact persistence.
- [x] 2.3 Wire native OCP evidence into existing report outputs while preserving generic template fallback.

## 3. Docs And Validation

- [x] 3.1 Update README and runtime docs with official OCP endpoint usage and config examples.
- [x] 3.2 Run targeted tests, `python3 -m unittest -v`, and `openspec validate --type change add-native-ocp-sql-diagnostics --strict --no-interactive`.

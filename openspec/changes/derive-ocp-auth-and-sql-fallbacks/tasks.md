## 1. OCP Auth And Target Resolution

- [x] 1.1 Add tests for generated Basic auth and name-based cluster or tenant resolution.
- [x] 1.2 Implement auth precedence and native OCP target resolution by name.

## 2. SQL Text Fallbacks

- [x] 2.1 Add tests for source SQL text recovery through local lookup, native OCP, and template fallback.
- [x] 2.2 Implement staged SQL text recovery and row-level source annotations.

## 3. Docs And Validation

- [x] 3.1 Update config template, runtime docs, and README for username/password-based OCP auth and cluster or tenant name settings.
- [x] 3.2 Run targeted tests, `python3 -m unittest -v`, and `openspec validate --type change derive-ocp-auth-and-sql-fallbacks --strict --no-interactive`.

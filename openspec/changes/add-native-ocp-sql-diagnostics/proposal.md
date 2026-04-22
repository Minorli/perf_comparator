## Why

The current OCP integration is only a generic URL-template hook, which is usable but not strong enough for production diagnosis. Now that we know the target OCP is `4.3.6`, the tool should understand official OCP SQL endpoints directly so operators do not have to reverse-engineer paths or hand-build templates.

## What Changes

- Add native OCP SQL diagnostics support using official OCP endpoints for `topSql`, `slowSql`, and SQL text lookup.
- Add OCP base URL, authorization-header env, cluster or tenant identifiers, time window, and TLS verification controls to config.
- Prefer native OCP collection when native settings are present, while keeping generic URL-template support as fallback.
- Persist native OCP evidence into report artifacts with concise summaries and referenced payload files.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `ob-replay-diagnostics`: optional OCP collection now supports a native provider for official OCP SQL endpoints instead of only generic templates.
- `performance-analysis-reporting`: reports now surface structured native OCP evidence when the provider is configured and a matching SQL is found.

## Impact

- Affected code: `perf_comparator.py`, `test_perf_comparator.py`, docs, config template
- External systems: OCP `api/v2` endpoints, operator-provided authorization header env
- Risks: OCP deployments may differ in auth and TLS settings, so the provider must degrade cleanly and preserve existing template-based behavior

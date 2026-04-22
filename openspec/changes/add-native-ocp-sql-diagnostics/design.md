## Context

`perf_comparator` already has optional external diagnostics, but its OCP support is intentionally generic: operators must provide handcrafted URL templates. That is too weak for repeated production use when we already know the official OCP SQL API surface and the target environment is OCP `4.3.6`.

The design still has to honor the existing constraints:

- Python 3.7 only
- single-file runtime
- no extra SDK dependencies
- graceful degradation over hard failure

## Goals / Non-Goals

**Goals:**

- Support native OCP SQL evidence collection with official `api/v2` endpoints.
- Let operators configure OCP once via base URL plus auth-header env and stable identifiers.
- Prefer native OCP mode automatically when enough config is present.
- Preserve generic template mode for special environments and backward compatibility.

**Non-Goals:**

- Full OCP object model traversal or dashboard replication.
- Automatic discovery of cluster or tenant IDs from names in this change.
- New CLI mode dedicated to OCP browsing.

## Decisions

### 1. Add a native OCP provider alongside the generic template provider

The runtime will keep generic template collection, but if `ocp_base_url`, auth-header env, cluster ID, and tenant ID are present, it will use a native provider first.

Why:

- preserves backward compatibility
- gives a stable “works out of the box” path for standard OCP deployments

### 2. Query list endpoints first, then fetch full SQL text when a hit is found

The native provider will query `topSql` and `slowSql` within a configurable time window using SQL text snippets, then fetch `/sql/{sqlId}/text` when a candidate is found.

Why:

- replay rows do not always carry a target-side OCP SQL ID directly
- SQL text search plus a short time window is robust enough for support workflows

### 3. Use an authorization-header env instead of a token-only env

The native provider will accept the full header value from an env var, such as `Basic ...` or `Bearer ...`.

Why:

- matches the user’s real OCP usage pattern
- avoids forcing one auth scheme

### 4. Add opt-in TLS verification bypass

Because the live environment currently uses `curl -k`, the provider will support `ocp_verify_tls=false`.

Why:

- production intranet OCP deployments often use private PKI
- this is necessary for parity with the operator’s existing working call pattern

## Risks / Trade-offs

- [SQL text matching may return multiple candidates] -> Mitigation: record both `topSql` and `slowSql` matches and surface the selected SQL ID in the evidence summary.
- [Cluster or tenant IDs may change across environments] -> Mitigation: keep IDs explicit in config and preserve generic fallback mode.
- [TLS verify bypass is risky] -> Mitigation: keep verification enabled by default and make the bypass explicit in config.
- [OCP response schemas may drift] -> Mitigation: parse defensively and persist raw payload files for operator inspection.

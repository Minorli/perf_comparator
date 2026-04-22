## Context

The current native OCP integration assumes three things that are still too operator-heavy:

1. the caller already produced a full `Authorization` header
2. the caller already knows `clusterId` and `tenantId`
3. SQL text recovery either comes directly from OB views or stays missing

The live OCP deployment already proved that Basic auth works and that `/api/v2/ob/clusters` returns both cluster and tenant names. That means the runtime can absorb more of this setup work.

## Goals / Non-Goals

**Goals:**

- Generate Basic auth automatically from username and password config.
- Resolve OCP cluster and tenant IDs from names when needed.
- Extend source-only SQL text recovery into a staged fallback pipeline.
- Preserve backward compatibility with the existing `ocp_authorization_env` and manual ID configuration.

**Non-Goals:**

- Support every OCP auth scheme beyond existing full-header mode and generated Basic auth.
- Add background caching daemons or persistent credential stores.
- Replace the current local OB SQL lookup path as the first choice.

## Decisions

### 1. Auth precedence is explicit and backward compatible

The runtime will resolve OCP auth in this order:

1. `ocp_authorization_env`
2. `ocp_username` + `ocp_password_env`
3. `ocp_username` + `ocp_password`

Why:

- keeps existing working setups unchanged
- lets operators move to username/password mode incrementally

### 2. Cluster and tenant resolution uses one cluster list call

The runtime will call `/api/v2/ob/clusters` and resolve both cluster and tenant IDs from names because the endpoint already includes embedded tenant objects.

Why:

- fewer requests
- avoids depending on a second endpoint unless necessary

### 3. SQL text recovery is staged and annotated

For source-only rows with missing SQL text, the runtime will try:

1. captured `QUERY_SQL`
2. privileged OB source lookup
3. native OCP SQL text lookup
4. generic OCP template fallback

Each row will record which step succeeded or that recovery still failed.

Why:

- prioritizes local and authoritative sources first
- uses OCP only when local SQL text is unavailable
- gives operators a traceable evidence chain

## Risks / Trade-offs

- [Config may include OCP password in plain text] -> Mitigation: support `ocp_password_env` and document it as the preferred secret path.
- [Cluster names may not be unique across environments] -> Mitigation: keep explicit ID settings as highest-certainty override.
- [OCP fallback lookups can add latency] -> Mitigation: only trigger OCP SQL recovery for rows whose SQL text is still missing.

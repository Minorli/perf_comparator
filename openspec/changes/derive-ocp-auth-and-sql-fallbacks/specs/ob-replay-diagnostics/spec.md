## ADDED Requirements

### Requirement: Generate native OCP Basic authentication from username and password
The native OCP provider SHALL generate a Basic authorization header when operators provide username and password inputs instead of a prebuilt header.

#### Scenario: Username and password are configured
- **GIVEN** native OCP mode is enabled
- **AND** the operator configured an OCP username and password or password env
- **WHEN** the runtime builds an OCP request
- **THEN** it generates `Authorization: Basic <base64(username:password)>`
- **AND** does not require a manually prebuilt authorization header

#### Scenario: Full authorization header env is still configured
- **GIVEN** native OCP mode is enabled
- **AND** the operator configured `ocp_authorization_env`
- **WHEN** the runtime builds an OCP request
- **THEN** it uses the provided full header value
- **AND** does not overwrite it with generated Basic auth

### Requirement: Resolve native OCP targets by cluster and tenant name
The native OCP provider SHALL resolve cluster and tenant IDs from names when explicit IDs are absent.

#### Scenario: Cluster and tenant names are configured
- **GIVEN** native OCP mode is enabled
- **AND** the operator configured `ocp_cluster_name` and `ocp_tenant_name`
- **WHEN** the runtime prepares an OCP SQL lookup
- **THEN** it queries the OCP cluster list endpoint
- **AND** resolves the matching cluster ID and tenant ID before querying SQL endpoints

#### Scenario: Name resolution fails
- **GIVEN** native OCP mode is enabled
- **AND** configured cluster or tenant names do not match the OCP response
- **WHEN** OCP collection is attempted
- **THEN** the runtime records a misconfigured or no-match status
- **AND** continues without blocking the rest of the workflow

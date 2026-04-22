## ADDED Requirements

### Requirement: Recover SQL text through multiple coordinated sources
The reporting path for source-only diagnosis SHALL attempt SQL text recovery through a staged fallback order when direct SQL text capture is unavailable.

#### Scenario: Privileged OB lookup succeeds
- **GIVEN** a source-only row has no visible SQL text
- **WHEN** privileged OB SQL lookup returns SQL text
- **THEN** the row records privileged lookup as the recovery source
- **AND** downstream reports show that the SQL text was backfilled locally

#### Scenario: OCP native lookup is needed
- **GIVEN** a source-only row has no visible SQL text
- **AND** privileged OB lookup did not recover it
- **WHEN** native OCP SQL lookup succeeds
- **THEN** the row records native OCP as the recovery source
- **AND** downstream reports show that SQL text came from OCP evidence

#### Scenario: All recovery sources fail
- **GIVEN** a source-only row has no visible SQL text
- **WHEN** all configured fallback sources fail
- **THEN** the row remains marked as missing
- **AND** the report preserves that failure state explicitly

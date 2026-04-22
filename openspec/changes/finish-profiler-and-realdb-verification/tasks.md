## 1. Config and Pipeline

- [x] 1.1 Allow `source_db_mode=oceanbase` to load without `[ORACLE_SOURCE]`.
- [x] 1.2 Add CLI and config controls for profiler diagnostics and real DB verification.
- [x] 1.3 Add tests covering OB-source mode without Oracle config.

## 2. Profiler Diagnostics

- [x] 2.1 Implement DBMS_PROFILER start/stop and run lookup helpers.
- [x] 2.2 Persist profiler hot-line evidence and attach summaries to replay rows.
- [x] 2.3 Add tests covering profiler evidence collection and graceful degradation.

## 3. Plan Translation and Reporting

- [x] 3.1 Implement translated operator-risk signals for Oracle -> OB plan changes.
- [x] 3.2 Surface plan-diff risk signals and profiler evidence in HTML/TXT/SQL-hint outputs.
- [x] 3.3 Add report tests covering translated operator-risk output.

## 4. Real DB Verification

- [x] 4.1 Implement single-file `verify-realdb` mode with reference-config import support.
- [x] 4.2 Add tests covering verification summary generation and safe package-deploy gating.
- [x] 4.3 Run live Oracle/OB verification using comparator-derived connection addresses and capture evidence.

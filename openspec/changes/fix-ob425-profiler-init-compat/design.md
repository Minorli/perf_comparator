## Context

The profiler pipeline was functionally correct in unit tests but failed in real DB validation on OceanBase 4.2.5 because the runtime used `CALL` for profiler helper procedures. Direct `obclient` testing showed:

- `CALL DBMS_PROFILER.OB_INIT_OBJECTS(FALSE)` fails
- `BEGIN DBMS_PROFILER.OB_INIT_OBJECTS(FALSE); END;` succeeds
- `BEGIN DBMS_PROFILER.START_PROFILER(...); DBMS_PROFILER.STOP_PROFILER(); END;` style wrappers also succeed

This is a concrete version compatibility issue, not a privilege problem.

## Goals / Non-Goals

**Goals**

- Make profiler init and start/stop SQL compatible with OB 4.2.5.
- Preserve the current single-file runtime and profiler behavior.
- Re-validate the complex package profiler smoke against the real target.

**Non-Goals**

- Reworking the profiler artifact schema
- Changing the diagnosis model

## Decisions

### 1. Use `BEGIN ... END;` wrappers for profiler procedure calls

Profiler helper SQL will use PL/SQL blocks instead of `CALL`.

Why:

- directly validated on OB 4.2.5
- remains readable and portable across the current runtime

### 2. Lock the wrapper form with tests

The unit tests will verify that profiler SQL generation contains the compatible wrapper structure.

Why:

- prevents reintroducing the same real-DB failure later

# Project Instructions

This repository uses OpenSpec as the source of truth for product planning and implementation.

## Required Workflow

1. Read `openspec/project.md` for project context, constraints, and roadmap.
2. Read the relevant baseline capability spec under `openspec/specs/` before proposing or implementing changes.
3. For any non-trivial behavior change, create an OpenSpec change first with `openspec new change <kebab-case-name>`.
4. Write or update `proposal.md`, `specs/`, `design.md`, and `tasks.md` inside `openspec/changes/<name>/` before editing implementation code.
5. Do not edit `openspec/specs/` directly for new work unless archiving a completed change or intentionally correcting the baseline.

## Current Environment Note

The repository includes OpenSpec-generated Codex skills under `.codex/skills/`.
If a future sandboxed session exposes `.codex` as read-only again, treat those files as bootstrap artifacts that should already exist in the repository rather than trying to regenerate them from inside the session.

## Baseline References

- Project context: `openspec/project.md`
- Core capabilities:
  - `openspec/specs/pipeline-orchestration/spec.md`
  - `openspec/specs/oracle-workload-capture/spec.md`
  - `openspec/specs/ob-replay-diagnostics/spec.md`
  - `openspec/specs/performance-analysis-reporting/spec.md`

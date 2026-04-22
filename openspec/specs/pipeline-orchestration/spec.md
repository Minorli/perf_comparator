# Pipeline Orchestration Specification

## Purpose

Define the command-line entrypoint that coordinates the Oracle capture, OceanBase replay, and reporting stages while allowing operators to rerun stages independently.

## Requirements

### Requirement: Support staged execution modes
The system SHALL provide a single CLI entrypoint that supports `batch`, `stream`, `replay-only`, and `report-only` execution modes.

#### Scenario: Batch mode runs the full pipeline
- GIVEN a valid Oracle source, OceanBase target, and runtime configuration
- WHEN an operator runs the CLI with `--mode batch`
- THEN the system captures workload data, replays that workload on OceanBase, and generates a performance report in one workflow

#### Scenario: Replay-only mode skips capture
- GIVEN an existing workload JSONL file
- WHEN an operator runs the CLI with `--mode replay-only --workload <file>`
- THEN the system skips Oracle capture
- AND replays the supplied workload file on OceanBase

#### Scenario: Report-only mode skips upstream stages
- GIVEN an existing replay JSONL file
- WHEN an operator runs the CLI with `--mode report-only --replay <file>`
- THEN the system skips workload capture and SQL replay
- AND regenerates report outputs from the supplied replay data

### Requirement: Expose operational control parameters
The CLI SHALL expose configuration flags for workload selection, timeout policy, replay cadence, reporting thresholds, and optional result-verification toggles.

#### Scenario: Operator tunes regression thresholds
- GIVEN an operator who wants to change report sensitivity
- WHEN the operator passes flags such as `--top-n`, `--min-exec`, `--hours`, `--timeout-factor`, or `--slowdown-threshold`
- THEN the system applies those values to the current run without requiring source-code changes

#### Scenario: Operator enables stream polling controls
- GIVEN an operator who wants continuous capture
- WHEN the operator passes `--mode stream` together with `--interval` or `--duration`
- THEN the system uses those values to control incremental polling behavior

### Requirement: Persist timestamped workflow artifacts
The system SHALL persist stage outputs as timestamped artifacts so that capture, replay, and reporting can be resumed or reanalyzed without rerunning every stage.

#### Scenario: Full workflow produces reusable artifacts
- GIVEN a completed end-to-end run
- WHEN the system finishes each stage
- THEN it stores workload, replay, audit-dump, and report artifacts using timestamped names
- AND later stages can consume those artifacts directly

#### Scenario: Operators inspect stage handoff files
- GIVEN a support engineer debugging a migration regression
- WHEN the engineer reviews generated artifacts
- THEN the files provide enough stage-level separation to isolate capture issues, replay issues, and reporting issues independently

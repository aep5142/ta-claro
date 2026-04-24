# Incremental Execution Workflow With Checkpoints

## Summary

Implement the ETL plan in gated phases. After each phase:

- write the code for that phase only
- write the tests for that phase in `tests/`
- run the relevant verification
- update `AGENTS.md` with the new durable project state
- stage changes with `git add`
- create a commit
- stop and ask for approval before starting the next phase

This keeps each phase as a clean checkpoint in code, tests, git history, and project context.

## Phase Workflow

For every phase, follow the same sequence:

1. Implement only the scoped work for that phase.
2. Write or update the tests required for that phase.
3. Run the phase-specific verification/tests.
4. Review changed files for scope discipline.
5. Update `AGENTS.md` with:
   - what was completed
   - any schema or worker behavior now in place
   - any decisions that became durable
   - any remaining next-step dependencies
6. Run:
   - `git add ...`
   - `git commit -m "..."` with a phase-specific message
7. Stop and request approval before moving to the next phase.

No phase should begin until explicitly approved.

## Testing Requirement

- Any new behavior introduced in a phase must ship with tests in that same phase.
- Tests live under the top-level `tests/` folder.
- A phase is not complete until its new tests pass, or a documented testing gap is explicitly accepted.
- Test files should be organized by ETL responsibility, for example:
  - `tests/workers/`
  - `tests/sources/`
  - `tests/transforms/`
  - `tests/loaders/`
  - `tests/fixtures/`

## Execution Phases

### Phase 1: Repo Foundation

- Create repo structure:
  - `data/`
  - `db/`
  - `front/`
  - `shared/`
  - `tests/`
- Create ETL subfolders under `data/`:
  - `workers/`
  - `sources/`
  - `transforms/`
  - `loaders/`
  - `models/`
- Add initial database SQL files for the CMF registry/state/raw/curated tables.
- No worker behavior changes yet.

Testing in this phase:

- Add only lightweight tests or validation scaffolding if Phase 1 introduces executable Python code.
- If Phase 1 is only structure and SQL assets, no behavioral tests are required yet.

Checkpoint requirements:

- commit Phase 1
- update `AGENTS.md` with repo structure and planned DB assets
- ask permission before Phase 2

### Phase 2: UF Worker Revision

- Refactor UF code into the new structure.
- Change UF cadence to every 5 days.
- Replace “after the 21st” logic with source-driven freshness logic.
- Keep UF isolated from CMF state/tables.
- Add UF tests.

Testing in this phase:

- latest source date detection
- no-op behavior when no new UF data exists
- incremental upsert behavior
- UF sync-state update behavior

Checkpoint requirements:

- commit Phase 2
- update `AGENTS.md` with new UF cadence, sync logic, and table/state expectations
- ask permission before Phase 3

### Phase 3: Transaction Count Pipeline

- Implement the transaction-count dataset end-to-end:
  - source fetch
  - parsing
  - `institution_code` derivation from `source_codigo`
  - raw upsert
  - curated upsert
- Add tests for parser, normalization, and idempotent load behavior.

Testing in this phase:

- response parsing
- `AGIFI` institution-code extraction
- month normalization
- raw upsert idempotency
- curated upsert idempotency

Checkpoint requirements:

- commit Phase 3
- update `AGENTS.md` with transaction dataset behavior and schema status
- ask permission before Phase 4

### Phase 4: Purchase Volume Pipeline

- Implement purchase-volume ingestion end-to-end.
- Add UF enrichment using UF from the 15th of the same month.
- Add tests for UF lookup and `real_volume_uf` calculation.

Testing in this phase:

- nominal CLP normalization
- UF lookup for the 15th of the month
- missing-UF failure behavior
- `real_volume_uf` calculation
- raw/curated upsert behavior

Checkpoint requirements:

- commit Phase 4
- update `AGENTS.md` with volume enrichment behavior and curated contract
- ask permission before Phase 5

### Phase 5: Shared CMF Worker

- Introduce the shared CMF monthly worker loop.
- Move the two datasets onto the shared orchestration path.
- Add freshness/state tests for unchanged vs newer source month.

Testing in this phase:

- unchanged source month produces no-op
- newer source month triggers sync
- failed sync does not advance CMF sync-state
- both datasets run correctly through the shared worker

Checkpoint requirements:

- commit Phase 5
- update `AGENTS.md` with shared worker behavior and CMF sync-state logic
- ask permission before Phase 6

### Phase 6: Query Surface And Hardening

- Add joined DB view(s) for downstream reads.
- Add operational logging/error-hardening.
- Finalize test coverage for the v1 path.

Testing in this phase:

- joined volume/transaction query surface
- expected join behavior on `institution_code + period_month`
- any practical integration coverage for the full v1 ETL path

Checkpoint requirements:

- commit Phase 6
- update `AGENTS.md` with final read surface and operational notes
- stop for review rather than assuming further work

## Git And Commit Rules

- One commit per phase.
- Commit messages should be phase-specific and descriptive.
- Do not mix work from the next phase into the current phase commit.
- If a phase grows too large, split it only with approval before changing the phase plan.

Recommended commit style:

- `phase 1: scaffold repo structure and db assets`
- `phase 2: revise uf worker to source-driven sync`
- `phase 3: add cmf transaction count pipeline`
- `phase 4: add cmf purchase volume pipeline`
- `phase 5: add shared cmf monthly worker`
- `phase 6: add joined query surface and hardening`

## AGENTS.md Update Rules

After each phase, update `AGENTS.md` with only durable facts:

- current repo structure
- implemented worker behavior
- implemented schema/tables/views
- implemented dataset coverage
- validated parsing/join rules
- current deployment-relevant behavior

Do not use `AGENTS.md` as a scratchpad for temporary thoughts.

## Approval Gate

After each committed phase, stop and ask one direct question:

- whether to proceed to the next phase

No autonomous continuation across phases. Approval is required every time.

## Assumptions

- Implementation should be checkpointed in git, not just logically phased.
- `AGENTS.md` is the durable handoff/state file between phases.
- Strict control over phase transitions is preferred even if that slows execution.

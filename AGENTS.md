# Project Context

This repo currently contains a UF ingestion worker in `data/historical_api_uf.py`.

# Runtime

- Python project managed with `uv`.
- Current UF entrypoint: `uv run data/historical_api_uf.py`
- The script expects a repo-root `.env`.

# External Services

- Supabase is the active database/backend.
- The UF worker uses `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY`.
- The UF worker fetches data from the CMF UF historical API using `CMF_API_KEY` and `CMF_UF_HISTORICAL`.
- The next ETL work also uses CMF endpoints served through `best-sbif-api.azurewebsites.net/Cuadrosv2`.

# Current UF State

- `data/historical_api_uf.py` is the current long-running async UF worker.
- Current local logic is still the old version:
  - retries every hour
  - waits until the 21st of the month
  - checks `public.uf_sync_runs` by `YYYY-MM`
  - upserts into `public.uf_values`
- This behavior is planned to change.

# Current Supabase Schema

- `public.uf_values`
  - `uf_date date primary key`
  - `value numeric not null`
- `public.uf_sync_runs`
  - `month_key text primary key`
  - `synced_at timestamptz not null default now()`

# Deployment Status

- The UF worker logic exists locally.
- Railway should run workers as worker services, not request/response web apps.
- No CMF card ETL worker is implemented yet.

# Frontend/Auth Direction

- The future website is expected to support authentication.
- Clerk is the likely auth provider, but no auth provider has been implemented or finalized yet.
- Do not add Clerk config, auth middleware, protected routes, user/session models, or auth tables until an auth-specific phase is approved.

# CMF Card ETL v1 Scope

- The first two project-demo ETL pulls are:
  - credit card purchase volume
  - number of transactions
- The first category family is bank credit cards.
- The first implementation focus is `Compras`.
- Start with monthly datasets, but keep the design extendable to mixed cadences later.

# Confirmed CMF Endpoints

- Purchase volume endpoint:
  - `https://best-sbif-api.azurewebsites.net/Cuadrosv2?FechaFin=YYYYMMDD&FechaInicio=20090401&Tag=SBIF_TCRED_BANC_COMP_AGIFI_$&from=reload`
- Transaction count endpoint:
  - `https://best-sbif-api.azurewebsites.net/Cuadrosv2?FechaFin=YYYYMMDD&FechaInicio=20090401&Tag=SBIF_TCRED_BANC_COMP_AGIFI_NUM&from=reload`
- `FechaInicio` stays fixed at `20090401`.
- `FechaFin` can be set to the run date. CMF still returns data only through the latest published month.

# Dataset Metadata Requirements

- Add a dataset metadata/config layer that stores at least:
  - `dataset_code`
  - `source_tag`
  - `source_nombre`
  - `source_description`
  - `source_endpoint_base`
  - `refresh_frequency`
  - `source_unit`
  - `start_date`
  - `is_active`

# Identity and Join Rules

- Numeric CMF `series.id` is not the cross-dataset join key.
- The stable join key for related CMF datasets is `institution_code`, derived from `source_codigo`.
- Rule for deriving `institution_code`:
  - split `source_codigo` by `_`
  - find `AGIFI`
  - take the next token
- Preserve both:
  - `source_series_id` as source metadata
  - `source_codigo` as source metadata
- For the v1 card datasets, join volume and transaction count on:
  - `institution_code`
  - `period_month`
- `institution_name` is descriptive/diagnostic, not the canonical join key.
- `institution_code` should be treated as reusable across related CMF datasets, but not as a guaranteed global identifier across all CMF families unless separately validated.

# Data Modeling Decisions

- Prefer a raw layer plus a curated layer.
- For v1, use raw and curated tables for both card datasets.
- Raw storage means parsed row-level source observations, not blob-only JSON archival.
- Curated means normalized and downstream-approved, not only transformed.
- Curated purchase volume should store at least:
  - `institution_code`
  - `institution_name`
  - `period_month`
  - `nominal_volume_clp`
  - `uf_date_used`
  - `uf_value_used`
  - `real_volume_uf`
- Curated transaction count should store at least:
  - `institution_code`
  - `institution_name`
  - `period_month`
  - `transaction_count`
- Volume must be converted to UF using the UF from the 15th day of the same month.
- Derived demo metrics should be computed at query time, not persisted in ETL tables.
- CMF endpoint numeric observations may arrive as strings; parser/normalization phases must explicitly convert them before loading numeric database columns.

# Current CMF Schema Assets

- Phase 1 added initial CMF database SQL in `db/001_cmf_foundation.sql`.
- The SQL defines CMF-specific tables only and does not alter UF tables:
  - `public.cmf_datasets`
  - `public.cmf_dataset_sync_state`
  - `public.cmf_card_transaction_count_raw`
  - `public.cmf_card_transaction_count_curated`
  - `public.cmf_card_purchase_volume_raw`
  - `public.cmf_card_purchase_volume_curated`
- `public.cmf_datasets` stores the initial active bank credit-card purchase-volume and transaction-count dataset metadata.
- CMF sync state is separate from UF sync state.
- CMF curated tables use `institution_code` plus `period_month` as the primary analytical grain for each dataset.

# UF Operational Direction

- Keep UF as a standalone subsystem separate from CMF dataset state.
- `uf_values` remains the conversion dependency table used by other datasets.
- Planned UF worker revision:
  - run every 5 days instead of the current hourly-after-21st rule
  - fetch source data
  - compare latest source UF date vs latest stored `uf_date`
  - if unchanged, do not upsert
  - if newer, upsert only new UF rows
- `uf_sync_runs` should be revised when implementing this new UF behavior so it matches the source-driven logic rather than the current month-marker logic.

# CMF Operational Direction

- For monthly CMF datasets, do not hardcode a publication day.
- Run the CMF monthly worker daily.
- For each active dataset:
  - fetch the endpoint with `FechaFin=today`
  - detect the latest source month in the payload
  - compare against the latest stored curated month
  - no-op when unchanged
  - sync only when a newer month appears
- Failed runs should not advance sync state.
- Keep CMF sync-state separate from UF sync-state.

# Demo Requirements

- The demo should support comparison by bank over a user-selected monthly date range.
- Show absolute values for:
  - real transaction volume
  - number of transactions
- Display units:
  - volume in millions of CLP in the UI
  - transaction count in thousands in the UI
- Also compute and show:
  - average ticket in CLP using today's UF
  - total growth for volume, transactions, and average ticket
  - CAGR for volume, transactions, and average ticket
- The UI/demo is expected to read from Supabase tables/views.

# Planned Repo Structure

- `data/`
  - `workers/`
  - `sources/`
  - `transforms/`
  - `loaders/`
  - `models/`
- `db/`
- `front/`
- `shared/`
- `tests/`

# Current Repo Structure

- Phase 1 created the planned top-level structure:
  - `data/`
  - `db/`
  - `front/`
  - `shared/`
  - `tests/`
- Phase 1 created ETL subfolders under `data/`:
  - `data/workers/`
  - `data/sources/`
  - `data/transforms/`
  - `data/loaders/`
  - `data/models/`
- The legacy UF worker remains at `data/historical_api_uf.py` until Phase 2.
- Empty structural directories are tracked with `.gitkeep` placeholders.

# Planned Testing Structure

- Keep tests in a top-level `tests/` folder.
- Organize tests by ETL responsibility:
  - `tests/workers/`
  - `tests/sources/`
  - `tests/transforms/`
  - `tests/loaders/`
  - `tests/fixtures/`

# Execution Workflow

- Work must be implemented in gated phases, not all at once.
- After each phase:
  - implement only the scoped work
  - write/update the tests for that phase
  - run the relevant verification
  - update `AGENTS.md` with durable facts
  - run `git add`
  - create one phase-specific commit
  - stop and ask for approval before the next phase
- No phase should begin until the user explicitly approves it.
- Any new behavior introduced in a phase must ship with tests in that same phase unless the user explicitly accepts a testing gap.

# Approved Phase Plan

## Phase 1: Repo Foundation

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
- Status: completed in the Phase 1 foundation commit.

## Phase 2: UF Worker Revision

- Refactor UF code into the new structure.
- Change UF cadence to every 5 days.
- Replace the “after the 21st” logic with source-driven freshness logic.
- Keep UF isolated from CMF state/tables.
- Add UF tests.

## Phase 3: Transaction Count Pipeline

- Implement the transaction-count dataset end-to-end:
  - source fetch
  - parsing
  - `institution_code` derivation from `source_codigo`
  - raw upsert
  - curated upsert
- Add tests for parser, normalization, and idempotent load behavior.

## Phase 4: Purchase Volume Pipeline

- Implement purchase-volume ingestion end-to-end.
- Add UF enrichment using UF from the 15th of the same month.
- Add tests for UF lookup and `real_volume_uf` calculation.

## Phase 5: Shared CMF Worker

- Introduce the shared CMF monthly worker loop.
- Move the two datasets onto the shared orchestration path.
- Add freshness/state tests for unchanged vs newer source month.

## Phase 6: Query Surface And Hardening

- Add joined DB view(s) for downstream reads.
- Add operational logging/error-hardening.
- Finalize test coverage for the v1 path.

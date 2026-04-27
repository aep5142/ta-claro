# Project Context

This repo currently contains a UF ingestion worker in `data/historical_api_uf.py`.

# Runtime

- Python project managed with `uv`.
- Current UF entrypoint: `uv run data/historical_api_uf.py`
- Current CMF cards entrypoint: `uv run data/api_bank_credit_card_operations.py`
- The script expects a repo-root `.env`.
- Tests use `pytest`.
- When adding Python dependencies, update both `pyproject.toml` and `requirements.txt`.

# External Services

- Supabase is the active database/backend.
- The UF worker uses `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY`.
- The UF worker fetches data from the CMF UF historical API using `CMF_API_KEY` and `BASE_ENDPOINT_CMF_UF`.
- The next ETL work also uses CMF endpoints served through `best-sbif-api.azurewebsites.net/Cuadrosv2`.
- The CMF card worker uses `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, and optional `BASE_ENDPOINT_CMF_CARDS`.

# Current UF State

- `data/historical_api_uf.py` remains the UF entrypoint.
- The UF worker is now refactored into:
  - `data/workers/uf_worker.py`
  - `data/sources/uf_source.py`
  - `data/loaders/uf_loader.py`
  - `data/models/uf.py`
- UF now runs on a 5-day loop.
- UF sync is source-driven:
  - fetch CMF historical UF source data
  - request the CMF UF endpoint two months ahead of the current month so the latest published UF date is exposed
  - compare latest source UF date against latest stored `public.uf_values.uf_date`
  - no-op when source is unchanged
  - upsert only UF rows newer than the latest stored date
- UF remains isolated from CMF dataset state and CMF tables.

# Current Supabase Schema

- `public.uf_values`
  - `uf_date date primary key`
  - `value numeric not null`
- `public.uf_sync_runs`
  - `sync_key text primary key`
  - `synced_at timestamptz not null default now()`
  - `latest_source_uf_date date`
  - `latest_stored_uf_date date`
  - `rows_upserted integer not null default 0`
  - `last_error text`
- `db/002_uf_source_driven_sync.sql` revises `public.uf_sync_runs` from monthly markers to singleton source-driven state.

# Deployment Status

- The UF worker logic exists locally.
- Railway should run workers as worker services, not request/response web apps.
- The CMF transaction-count pipeline exists locally as a one-shot worker path.
- The CMF purchase-volume pipeline exists locally as a one-shot worker path.
- The shared CMF daily worker loop exists locally for the two active card datasets.

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
  - `nominal_volume_millions_clp` in the raw table
  - `nominal_volume_thousands_millions_clp` in the curated table
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

# Current CMF Transaction Count State

- Phase 3 implemented the bank credit-card transaction-count pipeline for dataset code `bank_credit_card_transaction_count`.
- Transaction-count source code lives in `data/sources/bank_credit_card_operations.py`.
- Transaction-count transform code lives in `data/transforms/cmf_transaction_count.py`.
- Transaction-count loader code lives in `data/loaders/cmf_transaction_count_loader.py`.
- Transaction-count one-shot worker code lives in `data/workers/cmf_transaction_count_worker.py`.
- The transaction-count source tag is `SBIF_TCRED_BANC_COMP_AGIFI_NUM`.
- The endpoint builder uses `FechaInicio=20090401`, `FechaFin=YYYYMMDD`, `Tag=SBIF_TCRED_BANC_COMP_AGIFI_NUM`, and `from=reload`.
- The parser derives `institution_code` from `source_codigo` by taking the token after `AGIFI`.
- The parser preserves `source_series_id`, `source_codigo`, and `source_nombre` in raw observations.
- The parser normalizes supported source periods to first-of-month `date` values.
- The parser converts CMF numeric values, including string-encoded values, into numeric transaction counts before load.
- Raw transaction-count upserts target `public.bank_credit_card_transaction_count_raw` using conflict key `dataset_code,source_codigo,period_month`.
- Curated transaction-count upserts target `public.bank_credit_card_transaction_count_curated` using conflict key `institution_code,period_month`.
- The Phase 3 one-shot worker skips rows whose `period_month` is not newer than the latest curated transaction-count month.
- Purchase-volume ingestion is implemented separately and should remain joined with transaction count only in later query/view work.

# Current CMF Purchase Volume State

- Phase 4 implemented the bank credit-card purchase-volume pipeline for dataset code `bank_credit_card_purchase_volume`.
- Purchase-volume source parsing shares `data/sources/bank_credit_card_operations.py`.
- Purchase-volume transform code lives in `data/transforms/cmf_purchase_volume.py`.
- Purchase-volume loader code lives in `data/loaders/cmf_purchase_volume_loader.py`.
- Purchase-volume one-shot worker code lives in `data/workers/cmf_purchase_volume_worker.py`.
- The purchase-volume source tag is `SBIF_TCRED_BANC_COMP_AGIFI_$`.
- The endpoint builder uses `FechaInicio=20090401`, `FechaFin=YYYYMMDD`, `Tag=SBIF_TCRED_BANC_COMP_AGIFI_$`, and `from=reload`.
- The parser derives `institution_code` from `source_codigo` by taking the token after `AGIFI`.
- The parser preserves `source_series_id`, `source_codigo`, and `source_nombre` in raw observations.
- The parser normalizes supported source periods to first-of-month `date` values.
- The parser converts CMF numeric values, including string-encoded values, into `nominal_volume_millions_clp` before load.
- Curated purchase-volume enrichment looks up `public.uf_values` using the 15th day of the same month and scales the stored curated volume to thousands of millions of CLP.
- Missing required UF values raise an error and should prevent loading curated purchase-volume rows for that sync.
- `real_volume_uf` is calculated from raw millions of CLP as `nominal_volume_millions_clp * 1000000 / uf_value_used`.
- Raw purchase-volume upserts target `public.bank_credit_card_purchase_volume_raw` using conflict key `dataset_code,source_codigo,period_month`.
- Curated purchase-volume upserts target `public.bank_credit_card_purchase_volume_curated` using conflict key `institution_code,period_month`.
- The Phase 4 one-shot worker skips rows whose `period_month` is not newer than the latest curated purchase-volume month.

# Current Shared CMF Worker State

- Phase 5 added the shared CMF monthly worker in `data/workers/bank_credit_card_operations_worker.py`.
- The shared CMF entrypoint is `data/api_bank_credit_card_operations.py`.
- The shared CMF worker runs daily by default.
- The active shared CMF datasets are:
  - `bank_credit_card_transaction_count`
  - `bank_credit_card_purchase_volume`
- For each dataset, the shared worker:
  - records an attempted sync in `public.cmf_dataset_sync_state`
  - fetches the source endpoint with `FechaFin=run_date`
  - detects latest source month from parsed source observations
  - compares it against `public.cmf_dataset_sync_state.latest_source_month`
  - no-ops when the source month is unchanged
  - runs the dataset-specific one-shot sync when the source month is newer
  - records success only after the dataset-specific sync succeeds
  - records failure without advancing source or curated month state
- After successful dataset syncs, the shared worker refreshes the stored `public.bank_credit_card_purchases_metrics` table.
- Shared CMF state remains separate from UF state.

# Current CMF Schema Assets

- Phase 1 added initial CMF database SQL in `db/001_cmf_foundation.sql`.
- Phase 6 cleanup SQL lives in `db/004_cmf_cards_cleanup.sql`.
- The SQL defines CMF-specific tables only and does not alter UF tables:
  - `public.cmf_datasets`
  - `public.cmf_dataset_sync_state`
  - `public.bank_credit_card_transaction_count_raw`
  - `public.bank_credit_card_transaction_count_curated`
  - `public.bank_credit_card_purchase_volume_raw`
  - `public.bank_credit_card_purchase_volume_curated`
  - `public.bank_credit_card_purchases_metrics`
- `public.cmf_datasets` stores the initial active bank credit-card purchase-volume and transaction-count dataset metadata.
- CMF sync state is separate from UF sync state.
- CMF curated tables use `institution_code` plus `period_month` as the primary analytical grain for each dataset.
- The public card read surface is `public.bank_credit_card_purchases`, which exposes `average_ticket_uf` from the stored metrics table and computes `average_ticket_clp_today` at query time.

# UF Operational Direction

- Keep UF as a standalone subsystem separate from CMF dataset state.
- `uf_values` remains the conversion dependency table used by other datasets.
- UF source parsing accepts CMF date strings in `DD-MM-YYYY`, `YYYY-MM-DD`, and `DD/MM/YYYY` formats.
- UF value parsing accepts Chilean-formatted numeric strings and numeric source values.

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
- Purchase-volume metrics persist `average_ticket_uf`; `average_ticket_clp_today` is derived at query time from the latest UF.

# Demo Requirements

- The demo should support comparison by bank over a user-selected monthly date range.
- Show absolute values for:
  - real transaction volume
  - number of transactions
- Display units:
  - volume in millions of CLP in the UI
  - transaction count in thousands in the UI
- Also compute and show:
  - average ticket in UF from the stored metrics table
  - average ticket in CLP using today's UF at query time
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
- Phase 2 moved UF implementation modules into the planned `data/` ETL subfolders.
- Phase 3 added CMF transaction-count source, transform, loader, model, and one-shot worker modules.
- Phase 4 added CMF purchase-volume transform, loader, and one-shot worker modules, and extended shared CMF card source/model code.
- Phase 5 added shared CMF sync-state loader and daily worker orchestration modules.
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
- Status: completed in the Phase 2 UF worker revision commit.

## Phase 3: Transaction Count Pipeline

- Implement the transaction-count dataset end-to-end:
  - source fetch
  - parsing
  - `institution_code` derivation from `source_codigo`
  - raw upsert
  - curated upsert
- Add tests for parser, normalization, and idempotent load behavior.
- Status: completed in the Phase 3 transaction-count pipeline commit.

## Phase 4: Purchase Volume Pipeline

- Implement purchase-volume ingestion end-to-end.
- Add UF enrichment using UF from the 15th of the same month.
- Add tests for UF lookup and `real_volume_uf` calculation.
- Status: completed in the Phase 4 purchase-volume pipeline commit.

## Phase 5: Shared CMF Worker

- Introduce the shared CMF monthly worker loop.
- Move the two datasets onto the shared orchestration path.
- Add freshness/state tests for unchanged vs newer source month.
- Status: completed in the Phase 5 shared CMF worker commit.

## Phase 6: Query Surface And Hardening

- Add joined DB view(s) for downstream reads.
- Add operational logging/error-hardening.
- Finalize test coverage for the v1 path.
- `db/004_cmf_cards_cleanup.sql` defines the renamed card tables, the stored `public.bank_credit_card_purchases_metrics` table, and the simplified public `public.bank_credit_card_purchases` view.
- UF sync failures now persist `last_error` in `public.uf_sync_runs`.
- CMF monthly sync failures now persist `last_error` in `public.cmf_dataset_sync_state`, and the shared worker continues other datasets when one fails.

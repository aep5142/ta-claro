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
- The legacy split CMF purchase-volume and transaction-count worker paths are retired and archived.
- The active CMF daily worker loop is the unified credit-card ops worker.

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

# Current CMF Ops State

- The canonical credit-card subsystem is now the unified ops pipeline in `data/workers/bank_credit_card_ops_worker.py` and `data/api_bank_credit_card_operations.py`.
- Canonical operation types are:
  - `Compras`
  - `Avance en Efectivo`
  - `Cargos por Servicio`
- Registry/config rows live in `public.cmf_datasets`.
- Sync state lives in `public.cmf_dataset_sync_state`.
- Raw monthly observations live in `public.bank_credit_card_ops_raw`.
- Curated monthly observations live in `public.bank_credit_card_ops_curated`.
- The public read surface is `public.bank_credit_card_ops_metrics`.
- Registry rows are endpoint-level:
  - one `transaction_count` row per operation type
  - one `nominal_volume` row per operation type
  - 6 rows total for the current 3 credit-card operation types
- Registry rows store:
  - `operation_type`
  - `measure_kind`
  - `source_tag`
- The source builder uses `FechaInicio` from the registry row, `FechaFin=run_date`, the operation-specific source tag, and `from=reload`.
- The parser derives `institution_code` from `source_codigo` by taking the token after `AGIFI`; this remains valid for all current operation endpoints.
- The parser preserves CMF source metadata and merges the transaction-count and nominal-volume observations into one monthly raw row per operation and bank.
- Raw monthly rows store `transaction_count` and `nominal_volume_millions_clp`.
- Curated monthly rows store `nominal_volume_thousands_millions_clp`, `uf_date_used`, `uf_value_used`, `real_value_uf`, and `average_ticket_uf`.
- `real_value_uf` and `average_ticket_uf` are stored in UF units according to the current ops transform contract.
- The public view exposes only the canonical stored fields and does not derive CLP convenience columns.
- The worker runs daily by default, groups active endpoint rows by `operation_type`, fetches both endpoint tags together, no-ops only when both endpoint source months are unchanged, and records success/failure on both endpoint sync-state rows.
- Shared ops state remains separate from UF state.

# Current CMF Schema Assets

- Phase 1 added initial CMF database SQL in `db/001_cmf_foundation.sql`.
- The ops foundation SQL defines:
  - `public.cmf_datasets`
  - `public.cmf_dataset_sync_state`
  - `public.bank_credit_card_ops_raw`
  - `public.bank_credit_card_ops_curated`
- `public.cmf_datasets` is now endpoint-grained and includes `measure_kind`.
- `public.cmf_dataset_sync_state` is also endpoint-grained and currently has 6 card-op rows, one per endpoint dataset code.
- The public read surface lives in `db/003_bank_credit_card_ops_views.sql`.
- `public.bank_credit_card_ops_curated` uses `dataset_code + institution_code + period_month` as the primary analytical grain.
- The public card read surface is `public.bank_credit_card_ops_metrics`, which exposes the canonical stored curated fields only.

# UF Operational Direction

- Keep UF as a standalone subsystem separate from CMF dataset state.
- `uf_values` remains the conversion dependency table used by other datasets.
- UF source parsing accepts CMF date strings in `DD-MM-YYYY`, `YYYY-MM-DD`, and `DD/MM/YYYY` formats.
- UF value parsing accepts Chilean-formatted numeric strings and numeric source values.

# CMF Operational Direction

- For monthly CMF ops datasets, do not hardcode a publication day.
- Run the ops worker daily.
- For each operation type:
  - read the active endpoint rows from `cmf_datasets`
  - fetch both endpoint tags with `FechaFin=today`
  - detect the latest source month for each endpoint separately
  - compare each endpoint against its own `cmf_dataset_sync_state` row
  - no-op only when both endpoint source months are unchanged
  - sync the unified raw/curated operation rows when either endpoint advances
- Failed runs should not advance sync state.
- Keep ops sync-state separate from UF sync-state.
- `average_ticket_uf` is stored in `public.bank_credit_card_ops_curated`.

# Demo Requirements

- The demo should support comparison by bank over a user-selected monthly date range.
- Show absolute values for:
  - real transaction volume
  - number of transactions
- Display units:
  - volume in millions of CLP in the UI
  - transaction count in thousands in the UI
- Also compute and show:
  - average ticket in UF from the stored curated table
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
- The unified ops refactor replaced the purchase-only split with `bank_credit_card_ops_*` tables, `data/workers/bank_credit_card_ops_worker.py`, and `db/003_bank_credit_card_ops_views.sql`.
- Deprecated split-CMF transforms, loaders, and workers are being moved under `archive/` folders inside their ETL layer directories.
- The active card ETL code path is:
  - `data/workers/bank_credit_card_ops_worker.py`
  - `data/loaders/bank_credit_card_ops_loader.py`
  - `data/loaders/bank_credit_card_ops_sync_state_loader.py`
  - `data/transforms/bank_credit_card_ops.py`
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
- The current production read surface is the unified `public.bank_credit_card_ops_metrics` view over `public.bank_credit_card_ops_curated`.
- UF sync failures now persist `last_error` in `public.uf_sync_runs`.
- Ops sync failures now persist `last_error` in `public.cmf_dataset_sync_state`, and the worker continues other operations when one fails.

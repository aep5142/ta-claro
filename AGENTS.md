# Project Context

This repo contains two active ETL subsystems:

- UF ingestion
- unified bank credit-card operations ingestion

# Runtime

- Python project managed with `uv`.
- The repo expects a root `.env`.
- Tests use `pytest`.
- When adding Python dependencies, update both `pyproject.toml` and `requirements.txt`.

# Active Entrypoints

- UF worker:
  - canonical entrypoint: `uv run data/historical_api_uf.py`
  - worker module: `data/workers/uf_worker.py`
- Bank credit-card ops worker:
  - canonical entrypoint: `uv run data/bank_credit_card_ops.py`
  - worker module: `data/workers/bank_credit_card_ops_worker.py`

# External Services

- Supabase is the active database/backend.
- UF worker env:
  - `SUPABASE_URL`
  - `SUPABASE_SERVICE_ROLE_KEY`
  - `CMF_API_KEY`
  - `BASE_ENDPOINT_CMF_UF`
- Bank credit-card ops worker env:
  - `SUPABASE_URL`
  - `SUPABASE_SERVICE_ROLE_KEY`
  - optional `BASE_ENDPOINT_CMF_CARDS`

# UF State

- UF stays isolated from CMF dataset state and CMF tables.
- UF worker modules:
  - `data/workers/uf_worker.py`
  - `data/sources/uf_source.py`
  - `data/loaders/uf_loader.py`
  - `data/models/uf.py`
- UF runs on a 5-day loop.
- UF sync is source-driven:
  - fetch CMF historical UF source data
  - request two months ahead so the latest published UF date is exposed
  - compare latest source UF date against latest stored `public.uf_values.uf_date`
  - no-op when source is unchanged
  - upsert only UF rows newer than the latest stored date
- UF source parsing accepts:
  - `DD-MM-YYYY`
  - `YYYY-MM-DD`
  - `DD/MM/YYYY`
- UF value parsing accepts Chilean-formatted numeric strings and numeric values.

# Bank Credit-Card Ops State

- The active card subsystem is the unified ops pipeline plus monthly card-base counts, all run by the same worker.
- Canonical operation types are:
  - `Compras`
  - `Avance en Efectivo`
  - `Cargos por Servicio`
- The active operations-rate category is:
  - `Operations Rate`
- All 6 current credit-card endpoints use `FechaInicio=20090401`.
- `FechaFin` is set to the run date.
- The source builder always uses `from=reload`.
- Card-base endpoints use the same CMF builder and share the same worker cycle.
- The parser derives `institution_code` from `source_codigo` by:
  - splitting on `_`
  - finding `AGIFI`
  - taking the next token
- This `AGIFI` rule is valid for all current credit-card operation endpoints.

# Active Card Worker Flow

- The worker reads active endpoint rows from `public.cmf_datasets`.
- Metadata is endpoint-grained:
  - one `transaction_count` row per operation type
  - one `nominal_volume` row per operation type
  - 6 rows total for the current 3 operations
- The same worker also reads the 4 card-base endpoint rows used to derive active-card totals and cards-with-operations totals.
- The worker groups endpoint rows by `operation_type`.
- For each operation type it:
  - fetches both endpoint tags
  - detects the latest source month for each endpoint
  - compares each endpoint against its own `public.cmf_dataset_sync_state` row
  - no-ops only when both endpoint source months are unchanged
  - writes unified raw rows
  - writes unified curated rows
  - records success or failure on both endpoint sync-state rows
- For `Operations Rate` it writes the monthly bank-level totals needed by the public operations-rate view.
- Failed runs do not advance sync state.
- Ops sync state remains separate from UF sync state.

# Active Card Source / Transform / Loader Modules

- Sources:
  - `data/sources/bank_credit_card_operations.py`
- Transforms:
  - `data/transforms/bank_credit_card_ops.py`
- Loaders:
  - `data/loaders/bank_credit_card_ops_loader.py`
  - `data/loaders/bank_credit_card_ops_sync_state_loader.py`
- Models:
  - `data/models/bank_credit_card_operations.py`

# Active Supabase Schema

- UF tables:
  - `public.uf_values`
  - `public.uf_sync_runs`
- Shared CMF orchestration tables:
  - `public.cmf_datasets`
  - `public.cmf_dataset_sync_state`
- Unified bank credit-card ops storage:
  - `public.bank_credit_card_ops_raw`
  - `public.bank_credit_card_ops_curated`
- Unified bank credit-card public read surface:
  - `public.bank_credit_card_ops_metrics` as a view
- Unified bank credit-card counts storage:
  - `public.bank_credit_card_counts_raw`
  - `public.bank_credit_card_counts_curated`
- Unified bank credit-card operations-rate public read surface:
  - `public.bank_credit_card_operations_rate_metrics` as a view

# Card Data Contract

- Raw ops rows store:
  - `operation_type`
  - `dataset_code`
  - `institution_code`
  - `institution_name`
  - `period_month`
  - `transaction_count`
  - `nominal_volume_millions_clp`
- Curated ops rows store:
  - `operation_type`
  - `dataset_code`
  - `institution_code`
  - `institution_name`
  - `period_month`
  - `transaction_count`
  - `nominal_volume_millions_clp`
  - `uf_date_used`
  - `uf_value_used`
  - `real_value_uf`
  - `average_ticket_uf`
  - `total_active_cards`
  - `operations_per_active_card`
  - `source_dataset_code`
  - `updated_at`
- Volume conversion rules:
  - raw nominal volume is in millions of CLP
  - curated nominal volume is in millions of CLP
  - UF lookup uses the 15th day of the same month
  - `real_value_uf = nominal_volume_millions_clp / uf_value_used`
  - `average_ticket_uf = real_value_uf / transaction_count * 1000000`
- Card-count rows store:
  - `dataset_code`
  - `institution_code`
  - `institution_name`
  - `period_month`
  - `card_count`
- Curated card-count rows store:
  - `active_cards_primary`
  - `active_cards_supplementary`
  - `total_active_cards`
  - `cards_with_operations_primary`
  - `cards_with_operations_supplementary`
  - `total_cards_with_operations`
  - `operations_rate`
- All stored timestamps should use Santiago de Chile time.

# Public Read Surface

- `public.bank_credit_card_ops_metrics` is a view only, not a persisted metrics table.
- It exposes the canonical stored curated fields, including `nominal_volume_millions_clp`, `total_active_cards`, and `operations_per_active_card`.
- It does not expose:
  - `average_ticket_clp_today`
  - `operations_rate`
- CLP convenience calculations should happen at query time outside the stored schema.
- `public.bank_credit_card_operations_rate_metrics` exposes the bank-month totals used by the `Operations Rate` route.

# SQL Assets

- `db/001_cmf_foundation.sql`
  - shared CMF registry/state tables
  - unified bank credit-card ops raw/curated tables
- `db/002_uf_source_driven_sync.sql`
  - UF singleton source-driven sync state
- `db/003_bank_credit_card_ops_views.sql`
  - unified public ops metrics view
- `db/004_drop_obsolete_credit_card_views.sql`
  - removes obsolete purchase-only views and tables from the old split design
- `db/005_drop_obsolete_credit_card_tables.sql`
  - removes obsolete split raw/curated tables
- `db/006_split_cmf_card_endpoint_metadata.sql`
  - splits shared CMF metadata into 6 endpoint rows
- `db/007_fix_card_ops_start_dates.sql`
  - normalizes all current credit-card endpoint `start_date` values to `2009-04-01`
- `db/008_credit_card_card_counts.sql`
  - creates the card-count tables and adds the 4 card-base datasets
- `db/009_credit_card_metrics_rollback.sql`
  - rolls curated ops volume back to millions, updates the metrics view, and adds the operations-rate public view

# Repo Structure

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

# Archive Policy

- Deprecated split-CMF code is kept under `archive/` folders inside ETL layer directories.
- Archived code is not part of the active runtime path.
- Railway should point only at the active entrypoints.

# Testing State

- Active card test coverage follows unified ops naming.
- Active source tests include a unified live-payload regression fixture for the current credit-card ops payload shape.
- Old split-CMF tests and fixtures have been removed from the active `tests/` tree.

# Deployment Notes

- Railway should run workers as worker services, not request/response web apps.
- Card worker deploy command:
  - `uv run data/bank_credit_card_ops.py`

# Frontend/Auth Direction

- The future website is expected to support authentication.
- Clerk is the likely auth provider, but no auth provider has been implemented or finalized yet.
- Do not add Clerk config, auth middleware, protected routes, user/session models, or auth tables until an auth-specific phase is approved.

# Frontend Demo State

- The `front/` app is an active Next.js + Tailwind demo shell that mirrors the Artificial Analysis layout.
- Frontend runtime:
  - install deps in `front/` with `npm install`
  - run locally with `npm run dev` from `front/`
  - validate with `npm run build` from `front/`
- Brand/navigation:
  - Ta-Claro logo in the top bar
  - primary sections: `Credit Cards`, `Debit Cards`, `Accounts`, `Loans`
- only `Credit Cards` is functional in v1
- `Debit Cards`, `Accounts`, and `Loans` are placeholders inside the shared shell
- credit-card subsections/routes:
  - `Purchases` at `/credit-cards/purchases`
  - `Cash Advances` at `/credit-cards/cash-advances`
  - `Fees` at `/credit-cards/fees`
- `Operations Rate` at `/credit-cards/operations-rate`
- Credit-card demo behavior:
  - analysis tab is route-shareable via the `view` query param
  - render `Market Share ($Volume)`, `Market Share (Transactions)`, and `Average Transaction (CLP)`
  - render `Operations per Active Card` on the operation pages
  - render `Total Active Cards`, `Total Cards with Operations`, and `Operations Rate` on the operations-rate page
  - main visualization is a multi-bank line chart over time
  - time range is month-based and displayed as `MM/YY`
  - default range is the last 12 months ending at the latest available month for the selected operation
  - users can select and deselect banks
  - chart points support hover/focus tooltip inspection
  - use UF-adjusted CLP volume for `$Volume`
  - default UF is the latest UF up to today in `America/Santiago`
  - allow a user-entered UF override for CLP conversions
  - money display uses integer values with `.` thousands separators
  - percentage display uses `,` as the decimal separator with 1 decimal place
  - frontend must auto-paginate Supabase reads for larger date ranges and must not treat missing rows as zero values
- Data access:
  - browser reads use Supabase anon/public key
  - frontend reads `public.bank_credit_card_ops_metrics` and `public.uf_values`
  - the browser path is public read-only; no login in v1

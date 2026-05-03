# Project Context

This repo has two active ETL subsystems and one active frontend demo:

- UF ingestion
- unified bank credit-card operations ingestion, including card-count totals
- `front/` Next.js demo shell

# Runtime

- Python is managed with `uv`.
- Root `.env` is required.
- Tests use `pytest`.
- When adding Python dependencies, update both `pyproject.toml` and `requirements.txt`.

# Active Entrypoints

- UF worker: `uv run data/historical_api_uf.py`
- Card worker: `uv run data/bank_credit_card_ops.py`

Primary worker modules:

- `data/workers/uf_worker.py`
- `data/workers/bank_credit_card_ops_worker.py`

# External Services

- Supabase is the active backend/database.
- UF env: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `CMF_API_KEY`, `BASE_ENDPOINT_CMF_UF`
- Card env: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, optional `BASE_ENDPOINT_CMF_CARDS`

# UF Rules

- UF is isolated from CMF dataset sync state and CMF card tables.
- UF modules:
  - `data/sources/uf_source.py`
  - `data/loaders/uf_loader.py`
  - `data/models/uf.py`
- UF runs on a 5-day loop.
- Sync is source-driven:
  - fetch historical UF from CMF
  - request two months ahead so the latest published UF date is visible
  - compare source latest date vs `public.uf_values.uf_date`
  - no-op if unchanged
  - upsert only rows newer than the latest stored date
- Accepted UF date formats: `DD-MM-YYYY`, `YYYY-MM-DD`, `DD/MM/YYYY`
- UF values may be Chilean-formatted numeric strings or numeric values.

# Card Pipeline Rules

- The active card subsystem is one unified worker covering:
  - monthly operation metrics
  - monthly card-base counts
  - operations-rate totals
- Canonical operation types:
  - `Compras`
  - `Avance en Efectivo`
  - `Cargos por Servicio`
- Card-count canonical operation type: `Total Activation Rate`
- All 6 current operation endpoints use:
  - `FechaInicio=20090401`
  - `FechaFin` = run date
  - `from=reload`
- Card-base endpoints use the same CMF builder and worker cycle.
- `institution_code` is derived from `source_codigo` by splitting on `_`, locating `AGIFI`, and taking the next token.

# Card Worker Flow

- Read active endpoint rows from `public.cmf_datasets`.
- Metadata is endpoint-grained:
  - one `transaction_count` row per operation type
  - one `nominal_volume` row per operation type
  - 6 rows total for the 3 active operations
- Also read the 4 card-count endpoint rows used to derive active cards and cards with operations.
- Group datasets by `operation_type`.
- For each operation type:
  - fetch both endpoint tags
  - detect the latest source month for each endpoint
  - compare each endpoint to its own `public.cmf_dataset_sync_state` row
  - no-op only if both source months are unchanged
  - write unified raw rows and unified curated rows
  - enrich curated ops rows with `total_active_cards` from `public.bank_credit_card_counts_curated`
  - record success/failure on both endpoint sync-state rows
- For `Total Activation Rate`, write the bank-month totals used by the public operations-rate view.
- Failed runs must not advance sync state.
- Ops sync state stays separate from UF sync state.
- Be careful to paginate Supabase/PostgREST reads; default page limits can silently truncate lookups.

# Active ETL Modules

- Sources:
  - `data/sources/bank_credit_card_operations.py`
- Transforms:
  - `data/transforms/bank_credit_card_ops.py`
- Loaders:
  - `data/loaders/bank_credit_card_ops_loader.py`
  - `data/loaders/bank_credit_card_ops_sync_state_loader.py`
- Models:
  - `data/models/bank_credit_card_operations.py`
  - `data/models/uf.py`

# Active Supabase Schema

- UF:
  - `public.uf_values`
  - `public.uf_sync_runs`
- Shared orchestration:
  - `public.cmf_datasets`
  - `public.cmf_dataset_sync_state`
- Card ops:
  - `public.bank_credit_card_ops_raw`
  - `public.bank_credit_card_ops_curated`
  - `public.bank_credit_card_ops_metrics` view
- Card counts / operations rate:
  - `public.bank_credit_card_counts_raw`
  - `public.bank_credit_card_counts_curated`
  - `public.bank_credit_card_operations_rate_metrics` view

# Data Contracts

- Raw ops rows: `operation_type`, `dataset_code`, `institution_code`, `institution_name`, `period_month`, `transaction_count`, `nominal_volume_millions_clp`
- Curated ops rows add: `uf_date_used`, `uf_value_used`, `real_value_uf`, `average_ticket_uf`, `total_active_cards`, `operations_per_active_card`, `source_dataset_code`, `updated_at`
- Card-count rows: `dataset_code`, `institution_code`, `institution_name`, `period_month`, `card_count`
- Curated card-count rows include:
  - `active_cards_primary`
  - `active_cards_supplementary`
  - `total_active_cards`
  - `cards_with_operations_primary`
  - `cards_with_operations_supplementary`
  - `total_cards_with_operations`
  - `operations_rate`
- All stored timestamps should use Santiago de Chile time.

Volume/UF rules:

- Raw and curated nominal volume are both stored in millions of CLP.
- UF lookup uses the 15th day of the same month.
- `real_value_uf = nominal_volume_millions_clp / uf_value_used`
- `average_ticket_uf = real_value_uf / transaction_count * 1000000`

# Public Read Surface

- `public.bank_credit_card_ops_metrics` is a view, not a persisted metrics table.
- It exposes canonical curated fields including `nominal_volume_millions_clp`, `total_active_cards`, and `operations_per_active_card`.
- It does not expose `average_ticket_clp_today` or `operations_rate`.
- CLP convenience calculations should happen at query time, outside stored schema.
- `public.bank_credit_card_operations_rate_metrics` exposes the bank-month totals for the activation-metrics route, including primary/supplementary card-count fields used in the browser.

# SQL Assets

Active migration set:

- `db/001_cmf_foundation.sql`
- `db/002_uf_source_driven_sync.sql`
- `db/003_bank_credit_card_ops_views.sql`
- `db/004_drop_obsolete_credit_card_views.sql`
- `db/005_drop_obsolete_credit_card_tables.sql`
- `db/006_split_cmf_card_endpoint_metadata.sql`
- `db/007_fix_card_ops_start_dates.sql`
- `db/008_credit_card_card_counts.sql`
- `db/009_credit_card_metrics_rollback.sql`
- `db/010_operations_rate_add_supplementary_fields.sql`
- `db/011_rename_operations_rate_to_total_activation_rate.sql`
- `db/012_operations_rate_view_add_cards_with_operations_fields.sql`

# Repo Structure

- Active runtime code lives in `data/`, `db/`, `front/`, `shared/`, and `tests/`.
- Deprecated split-CMF code is kept under `archive/` directories and is not part of the active runtime path.

# Testing And Deployment

- Active card tests use unified ops naming.
- Source tests include a unified live-payload regression fixture for the current card ops payload shape.
- Old split-CMF tests/fixtures are removed from active `tests/`.
- Railway should run workers as worker services, not web apps.
- Card worker deploy command: `uv run data/bank_credit_card_ops.py`

# Frontend Direction

- `front/` is an active Next.js + Tailwind demo shell.
- Install with `npm install` in `front/`.
- Run with `npm run dev` in `front/`.
- Validate with `npm run build` in `front/`.
- Future auth is expected, and Clerk is the likely provider, but auth is not approved yet.
- Do not add Clerk config, auth middleware, protected routes, user/session models, or auth tables until an auth phase is explicitly approved.

# Frontend Product Rules

- Top bar uses the Ta-Claro logo.
- Primary sections are `Credit Cards`, `Debit Cards`, `Accounts`, `Loans`.
- Only `Credit Cards` is functional in v1; the others are placeholders inside the shared shell.
- If asked to build debit cards, first ask for debit-card metrics and source endpoints before planning implementation.
- Debit-card work should reuse the credit-card frontend pattern and interaction model rather than redesigning the shell.

Credit-card routes:

- `/credit-cards/purchases`
- `/credit-cards/cash-advances`
- `/credit-cards/fees`
- `/credit-cards/total-activation-rate`
- `/credit-cards/operations-rate` redirects to `/credit-cards/total-activation-rate` preserving `view`

Current shell/UI constraints:

- Top navbar is centered, text-first, with underline active state and a visual-only `Login` CTA.
- Left sidebar is minimalist text nav; Credit Cards shows the operation subroutes.
- Bank selection lives under the chart in `Banks shown` with `All`, `None`, and `Reset`.
- Bottom summary table includes `VS Start` and preserves the `Others` row behavior.
- Chart controls and rendering should stay aligned with the restored `origin/main` implementation.
- Sidebar uses a `Credit Cards` macro title and no `Live` badges.
- Dashboard copy should describe the product, not repeat the shareable route.
- Layout should use full width without requiring horizontal chart scroll.
- Mobile baseline is required:
  - no text overlap/clipping at small widths
  - no forced page-level horizontal overflow on phone screens
  - controls remain tappable and readable down to ~320px width
- On screens below `lg`, Credit Cards navigation/inputs use a collapsible drawer opened from the top bar `Menu` button.
- On `lg` and above, keep the current sticky left sidebar behavior.
- Mobile top nav uses a horizontally scrollable section row (`Credit Cards`, `Debit Cards`, `Accounts`, `Loans`) while desktop keeps centered nav.
- Chart tooltips should stay inside viewport bounds on small screens.
- Summary table may use local horizontal overflow as a safety fallback, but should use compact spacing on small screens before overflow is needed.

Credit-card behavior:

- Analysis tab is shareable via the `view` query param.
- Operation pages expose `Volume`, `Transactions`, `Avg. Transaction`, and `Operations per Active Card`.
- Operation Metrics page exposes `Total Active Cards`, `Total Cards with Operations`, `Total Activation Rate`, `Primary Activation Rate`, `Supplementary Activation Rate`, and `Supplementary Rate`.
- Main visualization is a multi-bank line chart over time.
- If the selected range is a single month, switch to a horizontal bar chart sorted descending.
- Bar labels stay outside bars with enough right-side space to avoid clipping.
- Default range is the last 12 months ending at the latest available month for the selected operation.
- Time range is month-based and displayed as `MM/YY`.
- Users can select/deselect banks.
- Bank labels come from `others/bank-mapping.txt`.
- Bank colors are deterministic from bank code.
- Point markers shrink for long date ranges.
- Tooltips support hover/focus inspection.
- Tooltip share line should show `XX% of the system` using system-wide month totals, not selected-bank totals; omit that line for `Transactions`.

Formatting and metric rules:

- `Volume ($)` uses UF-adjusted CLP volume.
- UF control label is `UF value`, uses a fixed `$` prefix, and formats thousands with `.`
- Default UF is the latest UF up to today in `America/Santiago`.
- User UF overrides must not reset bank selection.
- Money values use a fixed `$` prefix and integer formatting with `.` thousands separators.
- Percentages use `,` as decimal separator with 1 decimal place.
- Omit last-visible/last-loaded month copy because Start/End already show the range.
- Share-based tables include an `Others` row.
- Bank selector pills show only the bank name.

Frontend data access:

- Browser reads use the Supabase anon/public key.
- Frontend reads:
  - `public.bank_credit_card_ops_metrics`
  - `public.bank_credit_card_operations_rate_metrics`
  - `public.uf_values`
- The browser path is public read-only; there is no login in v1.
- Frontend must auto-paginate Supabase reads for larger date ranges and must not treat missing rows as zero values.

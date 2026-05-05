# Project Context

This repo has three active ETL subsystems and one active frontend demo:

- UF ingestion
- unified bank credit-card operations ingestion, including card-count totals
- unified bank debit-card and ATM-only-card operations ingestion, including combined card-count totals
- `front/` Next.js demo shell

# Runtime

- Python is managed with `uv`.
- Root `.env` is required.
- Tests use `pytest`.
- When adding Python dependencies, update both `pyproject.toml` and `requirements.txt`.

# Active Entrypoints

- UF worker: `uv run data/historical_api_uf.py`
- Credit-card worker: `uv run data/bank_credit_card_ops.py`
- Debit-card worker: `uv run data/bank_debit_card_ops.py`

Primary worker modules:

- `data/workers/uf_worker.py`
- `data/workers/bank_credit_card_ops_worker.py`
- `data/workers/bank_debit_card_ops_worker.py`

# External Services

- Supabase is the active backend/database.
- UF env: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `CMF_API_KEY`, `BASE_ENDPOINT_CMF_UF`
- Card env (credit and debit): `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, optional `BASE_ENDPOINT_CMF_CARDS`

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
- For non-banking card tags that use `AGIFI_MRC`, do not use plain `MRC` as the institution key; derive a per-series key from the trailing `source_codigo` tokens (for example `TENPO_MCRD`) to avoid issuer collisions.

# Debit Pipeline Rules

- The debit subsystem is one unified worker covering:
  - monthly debit transaction metrics
  - monthly ATM withdrawal metrics
  - monthly combined debit + ATM-only card-base totals
- Canonical operation types:
  - `Debit Transactions`
  - `ATM Withdrawals`
- Operation-metrics canonical operation type: `Total Activation Rate`
- Debit operation endpoints use:
  - `FechaInicio=20090401`
  - `FechaFin` = run date
  - `from=reload`
- Combined card-base logic must sum debit-card and ATM-only datasets:
  - primary active cards
  - supplementary active cards
  - total active cards
  - cards with operations
- Canonical ratios:
  - `operations_rate = total_cards_with_operations / total_active_cards`
  - `supplementary_rate = active_cards_supplementary / active_cards_primary`
- Failed runs must not advance sync state.
- Debit ops sync state stays separate from UF and credit sync state rows.

# Card Worker Flow

- Read active endpoint rows from `public.cmf_datasets`.
- Metadata is endpoint-grained:
  - one `transaction_count` row per operation type
  - one `nominal_volume` row per operation type
  - 6 rows total for the 3 active operations
- Also read the 4 card-count endpoint rows used to derive active cards and cards with operations.
- Also read the 2 non-banking card-count endpoint rows used to extend active cards and cards with operations totals.
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
  - `data/sources/bank_debit_card_operations.py`
- Transforms:
  - `data/transforms/bank_credit_card_ops.py`
  - `data/transforms/bank_debit_card_ops.py`
- Loaders:
  - `data/loaders/bank_credit_card_ops_loader.py`
  - `data/loaders/bank_debit_card_ops_loader.py`
  - `data/loaders/bank_credit_card_ops_sync_state_loader.py`
- Models:
  - `data/models/bank_credit_card_operations.py`
  - `data/models/bank_debit_card_operations.py`
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
- Debit ops:
  - `public.bank_debit_card_ops_raw`
  - `public.bank_debit_card_ops_curated`
  - `public.bank_debit_card_ops_metrics` view
- Card counts / operations rate:
  - `public.bank_credit_card_counts_raw`
  - `public.bank_credit_card_counts_curated`
  - `public.bank_credit_card_operations_rate_metrics` view
- Debit counts / operation metrics:
  - `public.bank_debit_card_counts_raw`
  - `public.bank_debit_card_counts_curated`
  - `public.bank_debit_card_operation_metrics` view

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
- `db/013_non_banking_credit_card_endpoints.sql`
- `db/014_debit_card_metrics.sql`

# Repo Structure

- Active runtime code lives in `data/`, `db/`, `front/`, `shared/`, and `tests/`.
- Deprecated split-CMF code is kept under `archive/` directories and is not part of the active runtime path.

# Testing And Deployment

- Active card tests use unified ops naming.
- Source tests include a unified live-payload regression fixture for the current card ops payload shape.
- Old split-CMF tests/fixtures are removed from active `tests/`.
- Railway should run workers as worker services, not web apps.
- Credit-card worker deploy command: `uv run data/bank_credit_card_ops.py`
- Debit-card worker deploy command: `uv run data/bank_debit_card_ops.py`
- Railway worker env var workaround (mise/aqua uv attestation check): set `MISE_AQUA_GITHUB_ATTESTATIONS=false`.

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
- `Credit Cards` and `Debit Cards` are functional in v1; `Accounts` and `Loans` remain placeholders.
- Debit-card work should reuse the credit-card frontend pattern and interaction model rather than redesigning the shell.

Credit-card routes:

- `/credit-cards/purchases`
- `/credit-cards/cash-advances`
- `/credit-cards/fees`
- `/credit-cards/total-activation-rate`
- `/credit-cards/operations-rate` redirects to `/credit-cards/total-activation-rate` preserving `view`

Debit-card routes:

- `/debit-cards/transactions`
- `/debit-cards/atm-withdrawals`
- `/debit-cards/total-activation-rate`

Current shell/UI constraints:

- Top navbar is centered, text-first, with underline active state and a visual-only `Login` CTA.
- Left sidebar is minimalist text nav; Credit Cards shows the operation subroutes.
- Bank selection lives under the chart in `Banks shown` with `All`, `None`, and `Reset`.
- Bottom summary table keeps the `Others` row for share-applicable metrics and uses month-explicit comparison headers.
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
- Default range is latest available month as `End` and the same calendar month in the previous year as `Start` (for example, `2026-02` to `2025-02`), clamped by the operation's earliest available month.
- Time range is month-based and displayed as `MM/YY`.
- Users can select/deselect banks.
- Bank labels come from `others/bank-mapping.txt`.
- Bank colors are deterministic from bank code.
- Point markers shrink for long date ranges.
- Tooltips support hover/focus inspection.
- Tooltip share line should show `XX% of the system` using system-wide month totals, not selected-bank totals; omit that line for `Transactions`.
- For non-banking issuers, frontend includes:
  - `Tenpo Payments S.A. - Tarjeta Mastercard`, displayed as `Tenpo`
  - the Promotora CMR Falabella issuer-brand rows, merged with `CMR Falabella S.A (SAG)` and displayed as `CMR Falabella`
- Other non-banking issuer-brand rows are filtered out in UI.

Debit-card behavior:

- Section title is `Debit Cards`, and description explicitly states it includes debit cards and ATM-only cards.
- Analysis tab is shareable via the `view` query param.
- Operation pages expose `Volume`, `Transactions`, `Avg. Transaction`, and `Operations per Active Card`.
- Operation Metrics page exposes:
  - `Total Active Cards`
  - `Total Cards with Operations`
  - `Total Activation Rate`
  - `Supplementary Rate`
- Debit operation metrics do not expose:
  - `Primary Activation Rate`
  - `Supplementary Activation Rate`
- Operational denominator for `Operations per Active Card` uses the combined debit + ATM-only active-card base.

Formatting and metric rules:

- `Volume ($)` uses UF-adjusted CLP volume.
- UF control label is `UF value`, uses a fixed `$` prefix, and formats thousands with `.`
- Default UF is the latest UF up to today in `America/Santiago`.
- User UF overrides must not reset bank selection.
- Money values use a fixed `$` prefix and integer formatting with `.` thousands separators.
- Percentages use `,` as decimal separator with 1 decimal place.
- Omit last-visible/last-loaded month copy because Start/End already show the range.
- Share-based tables include an `Others` row.
- In share-applicable operation views (`Volume`, `Transactions`), table columns are:
  - `Bank`
  - `<Metric>`
  - `Growth <Metric> <End> vs <Start>` (percentage growth)
  - `Market Share <End>`
  - `Market Share <End> vs <Start>` (absolute pp delta with direction arrow)
- If a bank has no start-month market-share row but has an end-month row, treat start-month market share as `0` for market-share pp delta.
- If a bank has no start-month metric row, do not compute `<Metric>` growth for that bank (show no value for growth).
- Date/input query updates in the sidebar must preserve scroll position (no jump to top while editing filters).
- Bank selector pills show only the bank name.
- `CAR S.A.` and `Banco Ripley` must be merged and displayed as `Banco Ripley`.

Frontend data access:

- Browser reads use the Supabase anon/public key.
- Frontend reads:
  - `public.bank_credit_card_ops_metrics`
  - `public.bank_credit_card_operations_rate_metrics`
  - `public.bank_debit_card_ops_metrics`
  - `public.bank_debit_card_operation_metrics`
  - `public.uf_values`
- The browser path is public read-only; there is no login in v1.
- Frontend must auto-paginate Supabase reads for larger date ranges and must not treat missing rows as zero values.

# Session Handoff (Debit Rollout)

- Debit rollout execution log is tracked in `final_plan_debit_cards.txt`.
- Completed phases:
  - Phase 0 (execution scaffold)
  - Phase 1 (database migration + SQL tests)
  - Phase 2 (backend domain/transforms/loaders + tests)
  - Phase 3 (sources/worker/entrypoint + tests)
  - Phase 4 (frontend config/query wiring + contract tests)
  - Phase 5 (frontend UI replication for debit routes)
  - Phase 6 (final verification and deployment prep)
- Current debit frontend status:
  - debit dashboard routes are fully wired (no placeholders)
  - market-share and growth table behavior follows current rules above

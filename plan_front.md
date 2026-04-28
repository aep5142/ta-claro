# Ta-Claro Frontend Plan

## Summary

Build a `front/` demo app that reproduces the structure of the Artificial Analysis models page, but with Ta-Claro branding and the project’s credit-card data model.

The first release is credit-cards only:
- `Credit Cards` is fully functional
- `Debit Cards`, `Accounts`, and `Loans` are visible navigation items but remain placeholders

## Key Changes

### 1. App shell
- Create a Next.js + Tailwind app in `front/`.
- Add a top navigation bar with the Ta-Claro logo and the four primary sections.
- Mirror the existing card-style navigation layout:
  - top section tabs
  - left sidebar
  - main analysis panel
- Keep the UI responsive, with the sidebar collapsing on smaller screens.

### 2. Credit-card navigation
- Implement `Credit Cards` as the only active section.
- Add three sub-options that stay in sync across the top dropdown and left menu:
  - `Purchases`
  - `Cash Advances`
  - `Fees`
- Route the credit-card pages by operation type so the selection is shareable/bookmarkable.

### 3. Data and analysis
- Read from Supabase using the browser anon/public key.
- Use `public.bank_credit_card_ops_metrics` as the main data source.
- Default to the latest available month for the selected operation.
- Render:
  - `Market Share ($Volume)`
  - `Market Share (Transactions)`
  - `Average Transaction (CLP)`
- For `$Volume`, use UF-adjusted CLP values:
  - `real_value_uf * UF`
  - default UF is the latest UF up to today in `America/Santiago`
  - user can override the UF value
- For `Average Transaction (CLP)`, use stored `average_ticket_uf` multiplied by the same UF value.
- Use `institution_name` for display and keep `institution_code` as the stable key.

### 4. Supabase access
- Read-only browser access uses:
  - `NEXT_PUBLIC_SUPABASE_URL`
  - `NEXT_PUBLIC_SUPABASE_ANON_KEY`
- The frontend reads:
  - `public.bank_credit_card_ops_metrics`
  - `public.uf_values`
- No login is used in v1.

## Test Plan

- Verify the shell renders the Ta-Claro logo and the four primary sections.
- Verify the Credit Cards dropdown and sidebar stay in sync with the route.
- Verify the latest-month query loads and the charts/tables render.
- Verify the UF default comes from the latest UF up to today in `America/Santiago`.
- Verify UF override changes the displayed CLP values immediately.
- Verify empty/error states do not crash the page.

## Assumptions

- Only the Credit Cards experience is implemented in v1.
- The demo uses public read access only, without authentication.
- “Today’s UF” means the latest UF row up to the current date in `America/Santiago`.
- `$Volume` is shown using UF-adjusted CLP, not the stored nominal CLP volume.

create or replace view public.bank_credit_card_ops_metrics as
select
    curated.operation_type,
    curated.dataset_code,
    curated.institution_code,
    curated.institution_name,
    curated.period_month,
    curated.transaction_count,
    curated.nominal_volume_thousands_millions_clp,
    curated.uf_date_used,
    curated.uf_value_used,
    curated.real_value_uf,
    curated.average_ticket_uf,
    curated.source_dataset_code,
    curated.updated_at
from public.bank_credit_card_ops_curated as curated;

comment on view public.bank_credit_card_ops_metrics
    is 'Public bank credit-card metrics view over the curated operations table.';

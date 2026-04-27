drop view if exists public.bank_credit_card_ops_metrics;

create or replace view public.bank_credit_card_ops_metrics as
with latest_uf as (
    select uf_date, value as latest_uf_value
    from public.uf_values
    order by uf_date desc
    limit 1
)
select
    curated.operation_type,
    curated.dataset_code,
    curated.institution_code,
    curated.institution_name,
    curated.period_month,
    curated.transaction_count,
    curated.nominal_volume_thousands_millions_clp,
    curated.nominal_volume_thousands_millions_clp * 1000 as nominal_volume_millions_clp,
    curated.uf_date_used,
    curated.uf_value_used,
    curated.real_value_uf,
    curated.average_ticket_uf,
    curated.average_ticket_uf * latest_uf.latest_uf_value as average_ticket_clp_today,
    curated.source_dataset_code,
    curated.updated_at
from public.bank_credit_card_ops_curated as curated
left join latest_uf on true;

comment on view public.bank_credit_card_ops_metrics
    is 'Public bank credit-card metrics view over the unified operations curated table.';

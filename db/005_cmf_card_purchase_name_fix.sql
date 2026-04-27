-- Rename the purchase metrics table/view to singular names.

alter table if exists public.bank_credit_card_purchases_metrics
    rename to bank_credit_card_purchase_metrics;

alter index if exists bank_credit_card_purchases_metrics_period_idx
    rename to bank_credit_card_purchase_metrics_period_idx;

create or replace view public.bank_credit_card_purchase as
with latest_uf as (
    select value as latest_uf_value
    from public.uf_values
    where uf_date = (
        select max(uf_date)
        from public.uf_values
    )
)
select
    metrics.institution_code,
    metrics.institution_name,
    metrics.period_month,
    metrics.nominal_volume_thousands_millions_clp,
    metrics.uf_date_used,
    metrics.uf_value_used,
    metrics.real_volume_uf,
    metrics.transaction_count,
    metrics.average_ticket_uf,
    case
        when metrics.average_ticket_uf is null then null
        when latest_uf.latest_uf_value is null then null
        else metrics.average_ticket_uf * latest_uf.latest_uf_value
    end as average_ticket_clp_today
from public.bank_credit_card_purchase_metrics as metrics
left join latest_uf on true;

-- Demo read surface for the CMF card ETL.
-- These views keep downstream queries on curated tables and compute demo-ready
-- measures at query time instead of persisting derived metrics in ETL tables.

create or replace view public.cmf_card_monthly_metrics as
select
    coalesce(volume.institution_code, transactions.institution_code) as institution_code,
    coalesce(volume.institution_name, transactions.institution_name) as institution_name,
    coalesce(volume.period_month, transactions.period_month) as period_month,
    volume.nominal_volume_clp,
    volume.uf_date_used,
    volume.uf_value_used,
    volume.real_volume_uf,
    transactions.transaction_count,
    volume.source_dataset_code as purchase_volume_source_dataset_code,
    transactions.source_dataset_code as transaction_count_source_dataset_code
from public.cmf_card_purchase_volume_curated as volume
full outer join public.cmf_card_transaction_count_curated as transactions
    on volume.institution_code = transactions.institution_code
   and volume.period_month = transactions.period_month;

create or replace view public.cmf_latest_uf as
select
    uf_date as latest_uf_date,
    value as latest_uf_value
from public.uf_values
where uf_date = (
    select max(uf_date)
    from public.uf_values
);

create or replace view public.cmf_card_monthly_demo_metrics as
select
    metrics.*,
    latest_uf.latest_uf_date,
    latest_uf.latest_uf_value,
    case
        when metrics.transaction_count is null or metrics.transaction_count = 0 then null
        when latest_uf.latest_uf_value is null then null
        else (metrics.real_volume_uf * latest_uf.latest_uf_value) / metrics.transaction_count
    end as average_ticket_clp_today
from public.cmf_card_monthly_metrics as metrics
left join public.cmf_latest_uf as latest_uf on true;

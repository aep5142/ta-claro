-- CMF card cleanup migration.
-- Keeps CMF registry/state tables intact, but renames the card tables,
-- stores the joined purchase metrics, and simplifies the public read surface.

alter table if exists public.cmf_card_transaction_count_raw
    rename to bank_credit_card_transaction_count_raw;

alter table if exists public.cmf_card_transaction_count_curated
    rename to bank_credit_card_transaction_count_curated;

alter table if exists public.cmf_card_purchase_volume_raw
    rename to bank_credit_card_purchase_volume_raw;

alter table if exists public.cmf_card_purchase_volume_curated
    rename to bank_credit_card_purchase_volume_curated;

alter table if exists public.bank_credit_card_purchase_volume_raw
    rename column nominal_volume_clp to nominal_volume_millions_clp;

alter table if exists public.bank_credit_card_purchase_volume_curated
    rename column nominal_volume_clp to nominal_volume_thousands_millions_clp;

drop view if exists public.cmf_card_monthly_demo_metrics;
drop view if exists public.cmf_latest_uf;
drop view if exists public.cmf_card_monthly_metrics;

create table if not exists public.bank_credit_card_purchases_metrics (
    institution_code text not null,
    institution_name text not null,
    period_month date not null,
    nominal_volume_thousands_millions_clp numeric not null,
    uf_date_used date not null,
    uf_value_used numeric not null,
    real_volume_uf numeric not null,
    transaction_count numeric not null,
    average_ticket_uf numeric not null,
    source_purchase_volume_dataset_code text not null references public.cmf_datasets(dataset_code),
    source_transaction_count_dataset_code text not null references public.cmf_datasets(dataset_code),
    updated_at timestamptz not null default (
        timezone('America/Santiago', now()) at time zone 'America/Santiago'
    ),
    primary key (institution_code, period_month)
);

create index if not exists bank_credit_card_purchases_metrics_period_idx
    on public.bank_credit_card_purchases_metrics (period_month);

comment on table public.bank_credit_card_purchase_volume_raw
    is 'Raw bank credit-card purchase volume in millions of CLP.';

comment on column public.bank_credit_card_purchase_volume_raw.nominal_volume_millions_clp
    is 'Monthly source purchase volume, expressed in millions of CLP.';

comment on table public.bank_credit_card_purchase_volume_curated
    is 'Curated bank credit-card purchase volume in thousands of millions of CLP.';

comment on column public.bank_credit_card_purchase_volume_curated.nominal_volume_thousands_millions_clp
    is 'Curated monthly purchase volume, expressed in thousands of millions of CLP.';

comment on table public.bank_credit_card_purchases_metrics
    is 'Stored joined card metrics with average ticket in UF.';

comment on column public.bank_credit_card_purchases_metrics.average_ticket_uf
    is 'Average ticket per transaction, expressed in UF.';

update public.cmf_datasets
set source_unit = 'millions CLP'
where dataset_code = 'bank_credit_card_purchase_volume';

create or replace view public.bank_credit_card_purchases as
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
from public.bank_credit_card_purchases_metrics as metrics
left join latest_uf on true;

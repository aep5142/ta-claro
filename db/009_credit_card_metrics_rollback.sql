alter table public.bank_credit_card_ops_curated
    rename column nominal_volume_thousands_millions_clp to nominal_volume_millions_clp;

alter table public.bank_credit_card_ops_curated
    add column if not exists total_active_cards numeric,
    add column if not exists operations_per_active_card numeric;

alter view public.bank_credit_card_ops_metrics
    rename column nominal_volume_thousands_millions_clp to nominal_volume_millions_clp;

comment on table public.bank_credit_card_ops_curated
    is 'Curated bank credit-card operation observations in millions of CLP.';

comment on column public.bank_credit_card_ops_curated.nominal_volume_millions_clp
    is 'Curated monthly nominal volume, expressed in millions of CLP.';

update public.cmf_datasets
set start_date = date '2009-04-01',
    updated_at = now()
where dataset_code in (
    'bank_credit_card_ops_compras_transaction_count',
    'bank_credit_card_ops_compras_nominal_volume',
    'bank_credit_card_ops_avance_en_efectivo_transaction_count',
    'bank_credit_card_ops_avance_en_efectivo_nominal_volume',
    'bank_credit_card_ops_cargos_por_servicio_transaction_count',
    'bank_credit_card_ops_cargos_por_servicio_nominal_volume',
    'bank_credit_card_active_cards_primary',
    'bank_credit_card_active_cards_supplementary',
    'bank_credit_card_cards_with_operations_primary',
    'bank_credit_card_cards_with_operations_supplementary'
);

create or replace view public.bank_credit_card_ops_metrics as
select
    curated.operation_type,
    curated.dataset_code,
    curated.institution_code,
    curated.institution_name,
    curated.period_month,
    curated.transaction_count,
    curated.nominal_volume_millions_clp,
    curated.uf_date_used,
    curated.uf_value_used,
    curated.real_value_uf,
    curated.average_ticket_uf,
    curated.source_dataset_code,
    curated.updated_at,
    curated.total_active_cards,
    curated.operations_per_active_card
from public.bank_credit_card_ops_curated as curated;

create or replace view public.bank_credit_card_operations_rate_metrics as
select
    counts.institution_code,
    counts.institution_name,
    counts.period_month,
    counts.total_active_cards,
    counts.total_cards_with_operations,
    counts.operations_rate
from public.bank_credit_card_counts_curated as counts
where counts.dataset_code = 'bank_credit_card_counts';

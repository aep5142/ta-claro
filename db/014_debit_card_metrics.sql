create table if not exists public.bank_debit_card_ops_raw (
    operation_type text not null,
    dataset_code text not null,
    source_series_id text not null,
    source_codigo text not null,
    source_nombre text not null,
    institution_code text not null,
    institution_name text not null,
    period_month date not null,
    transaction_count numeric not null,
    nominal_volume_millions_clp numeric not null,
    source_payload jsonb,
    ingested_at timestamptz not null default now(),
    primary key (dataset_code, source_codigo, period_month)
);

create table if not exists public.bank_debit_card_ops_curated (
    operation_type text not null,
    dataset_code text not null,
    institution_code text not null,
    institution_name text not null,
    period_month date not null,
    transaction_count numeric not null,
    nominal_volume_millions_clp numeric not null,
    uf_date_used date not null,
    uf_value_used numeric not null,
    real_value_uf numeric not null,
    average_ticket_uf numeric not null,
    total_active_cards numeric,
    operations_per_active_card numeric,
    source_dataset_code text not null,
    updated_at timestamptz not null default now(),
    primary key (dataset_code, institution_code, period_month)
);

create table if not exists public.bank_debit_card_counts_raw (
    dataset_code text not null,
    source_series_id text not null,
    source_codigo text not null,
    source_nombre text not null,
    institution_code text not null,
    institution_name text not null,
    period_month date not null,
    card_count numeric not null,
    source_payload jsonb,
    ingested_at timestamptz not null default now(),
    primary key (dataset_code, source_codigo, period_month)
);

create table if not exists public.bank_debit_card_counts_curated (
    dataset_code text not null,
    institution_code text not null,
    institution_name text not null,
    period_month date not null,
    active_cards_primary numeric not null,
    active_cards_supplementary numeric not null,
    total_active_cards numeric not null,
    total_cards_with_operations numeric not null,
    operations_rate numeric,
    updated_at timestamptz not null default now(),
    primary key (dataset_code, institution_code, period_month)
);

create index if not exists bank_debit_card_ops_raw_period_idx
    on public.bank_debit_card_ops_raw (period_month);

create index if not exists bank_debit_card_ops_curated_period_idx
    on public.bank_debit_card_ops_curated (period_month);

create index if not exists bank_debit_card_counts_raw_period_idx
    on public.bank_debit_card_counts_raw (period_month);

create index if not exists bank_debit_card_counts_curated_period_idx
    on public.bank_debit_card_counts_curated (period_month);

comment on table public.bank_debit_card_ops_raw
    is 'Raw debit-card and ATM-only operation observations in millions of CLP.';

comment on table public.bank_debit_card_ops_curated
    is 'Curated debit-card and ATM-only operation observations in millions of CLP.';

comment on table public.bank_debit_card_counts_curated
    is 'Curated combined debit-card and ATM-only active-card metrics by institution and month.';

insert into public.cmf_datasets (
    dataset_code,
    operation_type,
    measure_kind,
    source_tag,
    source_nombre,
    source_description,
    source_endpoint_base,
    refresh_frequency,
    source_unit,
    start_date,
    is_active
) values
    (
        'bank_debit_card_ops_debit_transactions_transaction_count',
        'Debit Transactions',
        'transaction_count',
        'SBIF_TDEB_TATM_OPER_TXDEB_AGIFI_NUM',
        'Transacciones de debito por emisor',
        'Monthly debit transaction counts by institution from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'count',
        date '2009-04-01',
        true
    ),
    (
        'bank_debit_card_ops_debit_transactions_nominal_volume',
        'Debit Transactions',
        'nominal_volume',
        'SBIF_TDEB_TATM_OPER_TXDEB_AGIFI_MM$',
        'Transacciones de debito por emisor',
        'Monthly debit transaction nominal volume by institution from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'millions_clp',
        date '2009-04-01',
        true
    ),
    (
        'bank_debit_card_ops_atm_withdrawals_transaction_count',
        'ATM Withdrawals',
        'transaction_count',
        'SBIF_TDEB_TATM_OPER_GIR_AGIFI_NUM',
        'Giros con tarjetas de debito por emisor',
        'Monthly ATM withdrawal transaction counts by institution from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'count',
        date '2009-04-01',
        true
    ),
    (
        'bank_debit_card_ops_atm_withdrawals_nominal_volume',
        'ATM Withdrawals',
        'nominal_volume',
        'SBIF_TDEB_TATM_OPER_GIR_AGIFI_MM$',
        'Giros con tarjetas de debito por emisor',
        'Monthly ATM withdrawal nominal volume by institution from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'millions_clp',
        date '2009-04-01',
        true
    ),
    (
        'bank_debit_card_active_cards_primary_debit',
        'Total Activation Rate',
        'active_cards_primary_debit',
        'SBIF_TDEB_VIGTIT_AGIFI_NUM',
        'Tarjetas de debito vigentes titulares por emisor',
        'Monthly active primary debit cards by institution from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'count',
        date '2009-04-01',
        true
    ),
    (
        'bank_debit_card_active_cards_primary_atm_only',
        'Total Activation Rate',
        'active_cards_primary_atm_only',
        'SBIF_TATM_VIGTIT_AGIFI_NUM',
        'Tarjetas solo ATM vigentes titulares por emisor',
        'Monthly active primary ATM-only cards by institution from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'count',
        date '2009-04-01',
        true
    ),
    (
        'bank_debit_card_active_cards_supplementary_debit',
        'Total Activation Rate',
        'active_cards_supplementary_debit',
        'SBIF_TDEB_VIGADIC_AGIFI_NUM',
        'Tarjetas de debito vigentes adicionales por emisor',
        'Monthly active supplementary debit cards by institution from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'count',
        date '2009-04-01',
        true
    ),
    (
        'bank_debit_card_active_cards_supplementary_atm_only',
        'Total Activation Rate',
        'active_cards_supplementary_atm_only',
        'SBIF_TATM_VIGADIC_AGIFI_NUM',
        'Tarjetas solo ATM vigentes adicionales por emisor',
        'Monthly active supplementary ATM-only cards by institution from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'count',
        date '2009-04-01',
        true
    ),
    (
        'bank_debit_card_active_cards_total_debit',
        'Total Activation Rate',
        'active_cards_total_debit',
        'SBIF_TDEB_VIG_AGIFI_NUM',
        'Tarjetas de debito vigentes totales por emisor',
        'Monthly total active debit cards by institution from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'count',
        date '2009-04-01',
        true
    ),
    (
        'bank_debit_card_active_cards_total_atm_only',
        'Total Activation Rate',
        'active_cards_total_atm_only',
        'SBIF_TATM_VIG_AGIFI_NUM',
        'Tarjetas solo ATM vigentes totales por emisor',
        'Monthly total active ATM-only cards by institution from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'count',
        date '2009-04-01',
        true
    ),
    (
        'bank_debit_card_cards_with_operations_debit',
        'Total Activation Rate',
        'cards_with_operations_debit',
        'SBIF_TDEB_COPE_AGIFI_NUM',
        'Tarjetas de debito con operaciones por emisor',
        'Monthly debit cards with operations by institution from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'count',
        date '2009-04-01',
        true
    ),
    (
        'bank_debit_card_cards_with_operations_atm_only',
        'Total Activation Rate',
        'cards_with_operations_atm_only',
        'SBIF_TATM_COPE_AGIFI_NUM',
        'Tarjetas solo ATM con operaciones por emisor',
        'Monthly ATM-only cards with operations by institution from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'count',
        date '2009-04-01',
        true
    )
on conflict (dataset_code) do update set
    operation_type = excluded.operation_type,
    measure_kind = excluded.measure_kind,
    source_tag = excluded.source_tag,
    source_nombre = excluded.source_nombre,
    source_description = excluded.source_description,
    source_endpoint_base = excluded.source_endpoint_base,
    refresh_frequency = excluded.refresh_frequency,
    source_unit = excluded.source_unit,
    start_date = excluded.start_date,
    is_active = excluded.is_active,
    updated_at = now();

create or replace view public.bank_debit_card_ops_metrics as
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
    curated.total_active_cards,
    curated.operations_per_active_card,
    curated.source_dataset_code,
    curated.updated_at
from public.bank_debit_card_ops_curated as curated;

create or replace view public.bank_debit_card_operation_metrics as
select
    counts.institution_code,
    counts.institution_name,
    counts.period_month,
    counts.active_cards_primary,
    counts.active_cards_supplementary,
    counts.total_active_cards,
    counts.total_cards_with_operations,
    counts.operations_rate
from public.bank_debit_card_counts_curated as counts
where counts.dataset_code = 'bank_debit_card_counts';

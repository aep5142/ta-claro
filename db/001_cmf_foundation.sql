-- CMF card ETL foundation.
-- This migration is intentionally separate from the existing UF tables.

create table if not exists public.cmf_datasets (
    dataset_code text primary key,
    source_tag text not null,
    source_nombre text not null,
    source_description text not null,
    source_endpoint_base text not null,
    refresh_frequency text not null,
    source_unit text not null,
    start_date date not null,
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.cmf_dataset_sync_state (
    dataset_code text primary key references public.cmf_datasets(dataset_code),
    latest_source_month date,
    latest_curated_month date,
    last_successful_sync_at timestamptz,
    last_attempted_sync_at timestamptz,
    last_error text,
    updated_at timestamptz not null default now()
);

create table if not exists public.cmf_card_transaction_count_raw (
    dataset_code text not null references public.cmf_datasets(dataset_code),
    source_series_id text not null,
    source_codigo text not null,
    source_nombre text not null,
    institution_code text not null,
    institution_name text not null,
    period_month date not null,
    transaction_count numeric not null,
    source_payload jsonb,
    ingested_at timestamptz not null default now(),
    primary key (dataset_code, source_codigo, period_month)
);

create table if not exists public.cmf_card_transaction_count_curated (
    institution_code text not null,
    institution_name text not null,
    period_month date not null,
    transaction_count numeric not null,
    source_dataset_code text not null references public.cmf_datasets(dataset_code),
    updated_at timestamptz not null default now(),
    primary key (institution_code, period_month)
);

create table if not exists public.cmf_card_purchase_volume_raw (
    dataset_code text not null references public.cmf_datasets(dataset_code),
    source_series_id text not null,
    source_codigo text not null,
    source_nombre text not null,
    institution_code text not null,
    institution_name text not null,
    period_month date not null,
    nominal_volume_clp numeric not null,
    source_payload jsonb,
    ingested_at timestamptz not null default now(),
    primary key (dataset_code, source_codigo, period_month)
);

create table if not exists public.cmf_card_purchase_volume_curated (
    institution_code text not null,
    institution_name text not null,
    period_month date not null,
    nominal_volume_clp numeric not null,
    uf_date_used date not null,
    uf_value_used numeric not null,
    real_volume_uf numeric not null,
    source_dataset_code text not null references public.cmf_datasets(dataset_code),
    updated_at timestamptz not null default now(),
    primary key (institution_code, period_month)
);

create index if not exists cmf_card_transaction_count_raw_period_idx
    on public.cmf_card_transaction_count_raw (period_month);

create index if not exists cmf_card_purchase_volume_raw_period_idx
    on public.cmf_card_purchase_volume_raw (period_month);

create index if not exists cmf_card_transaction_count_curated_period_idx
    on public.cmf_card_transaction_count_curated (period_month);

create index if not exists cmf_card_purchase_volume_curated_period_idx
    on public.cmf_card_purchase_volume_curated (period_month);

insert into public.cmf_datasets (
    dataset_code,
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
        'bank_credit_card_purchase_volume',
        'SBIF_TCRED_BANC_COMP_AGIFI_$',
        'Compras con tarjetas de credito bancarias por institucion',
        'Monthly bank credit card purchase volume by institution from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'CLP',
        date '2009-04-01',
        true
    ),
    (
        'bank_credit_card_transaction_count',
        'SBIF_TCRED_BANC_COMP_AGIFI_NUM',
        'Numero de compras con tarjetas de credito bancarias por institucion',
        'Monthly bank credit card transaction count by institution from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'transactions',
        date '2009-04-01',
        true
    )
on conflict (dataset_code) do update set
    source_tag = excluded.source_tag,
    source_nombre = excluded.source_nombre,
    source_description = excluded.source_description,
    source_endpoint_base = excluded.source_endpoint_base,
    refresh_frequency = excluded.refresh_frequency,
    source_unit = excluded.source_unit,
    start_date = excluded.start_date,
    is_active = excluded.is_active,
    updated_at = now();

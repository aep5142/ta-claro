-- CMF foundation with shared orchestration tables.
-- This migration is intentionally separate from the existing UF tables.

create table if not exists public.cmf_datasets (
    dataset_code text primary key,
    operation_type text,
    source_tag text not null,
    transaction_count_source_tag text,
    nominal_volume_source_tag text,
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

create table if not exists public.bank_credit_card_ops_raw (
    operation_type text not null,
    dataset_code text not null references public.cmf_datasets(dataset_code),
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

create table if not exists public.bank_credit_card_ops_curated (
    operation_type text not null,
    dataset_code text not null references public.cmf_datasets(dataset_code),
    institution_code text not null,
    institution_name text not null,
    period_month date not null,
    transaction_count numeric not null,
    nominal_volume_thousands_millions_clp numeric not null,
    uf_date_used date not null,
    uf_value_used numeric not null,
    real_value_uf numeric not null,
    average_ticket_uf numeric not null,
    source_dataset_code text not null references public.cmf_datasets(dataset_code),
    updated_at timestamptz not null default now(),
    primary key (dataset_code, institution_code, period_month)
);

create index if not exists bank_credit_card_ops_raw_period_idx
    on public.bank_credit_card_ops_raw (period_month);

create index if not exists bank_credit_card_ops_curated_period_idx
    on public.bank_credit_card_ops_curated (period_month);

comment on table public.bank_credit_card_ops_raw
    is 'Raw bank credit-card operation observations in millions of CLP.';

comment on column public.bank_credit_card_ops_raw.nominal_volume_millions_clp
    is 'Monthly nominal volume from CMF, expressed in millions of CLP.';

comment on table public.bank_credit_card_ops_curated
    is 'Curated bank credit-card operation observations in thousands of millions of CLP.';

comment on column public.bank_credit_card_ops_curated.nominal_volume_thousands_millions_clp
    is 'Curated monthly nominal volume, expressed in thousands of millions of CLP.';

comment on column public.bank_credit_card_ops_curated.real_value_uf
    is 'Curated monthly nominal value converted to UF.';

insert into public.cmf_datasets (
    dataset_code,
    operation_type,
    source_tag,
    transaction_count_source_tag,
    nominal_volume_source_tag,
    source_nombre,
    source_description,
    source_endpoint_base,
    refresh_frequency,
    source_unit,
    start_date,
    is_active
) values
    (
        'bank_credit_card_ops_compras',
        'Compras',
        'SBIF_TCRED_BANC_COMP_AGIFI',
        'SBIF_TCRED_BANC_COMP_AGIFI_NUM',
        'SBIF_TCRED_BANC_COMP_AGIFI_$',
        'Compras con tarjetas de credito bancarias por institucion',
        'Monthly bank credit card purchases by institution from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'combined',
        date '2009-04-01',
        true
    ),
    (
        'bank_credit_card_ops_avance_en_efectivo',
        'Avance en Efectivo',
        'SBIF_TCRED_BANC_AVEF_AGIFI',
        'SBIF_TCRED_BANC_AVEF_AGIFI_NUM',
        'SBIF_TCRED_BANC_AVEF_AGIFI_$',
        'Avance en efectivo con tarjetas de credito bancarias por institucion',
        'Monthly bank credit card cash advances by institution from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'combined',
        date '2009-04-01',
        true
    ),
    (
        'bank_credit_card_ops_cargos_por_servicio',
        'Cargos por Servicio',
        'SBIF_TCRED_BANC_CSERV_AGIFI',
        'SBIF_TCRED_BANC_CSERV_AGIFI_NUM',
        'SBIF_TCRED_BANC_CSERV_AGIFI_$',
        'Cargos por servicio con tarjetas de credito bancarias por institucion',
        'Monthly bank credit card service charges by institution from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'combined',
        date '2009-04-01',
        true
    )
on conflict (dataset_code) do update set
    operation_type = excluded.operation_type,
    source_tag = excluded.source_tag,
    transaction_count_source_tag = excluded.transaction_count_source_tag,
    nominal_volume_source_tag = excluded.nominal_volume_source_tag,
    source_nombre = excluded.source_nombre,
    source_description = excluded.source_description,
    source_endpoint_base = excluded.source_endpoint_base,
    refresh_frequency = excluded.refresh_frequency,
    source_unit = excluded.source_unit,
    start_date = excluded.start_date,
    is_active = excluded.is_active,
    updated_at = now();

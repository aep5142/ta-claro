create table if not exists public.bank_credit_card_counts_raw (
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

create table if not exists public.bank_credit_card_counts_curated (
    dataset_code text not null,
    institution_code text not null,
    institution_name text not null,
    period_month date not null,
    active_cards_primary numeric not null,
    active_cards_supplementary numeric not null,
    total_active_cards numeric not null,
    cards_with_operations_primary numeric not null,
    cards_with_operations_supplementary numeric not null,
    total_cards_with_operations numeric not null,
    operations_rate numeric,
    updated_at timestamptz not null default now(),
    primary key (dataset_code, institution_code, period_month)
);

create index if not exists bank_credit_card_counts_raw_period_idx
    on public.bank_credit_card_counts_raw (period_month);

create index if not exists bank_credit_card_counts_curated_period_idx
    on public.bank_credit_card_counts_curated (period_month);

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
        'bank_credit_card_active_cards_primary',
        'Operations Rate',
        'active_cards_primary',
        'SBIF_TCRED_BANC_VIGTIT_AGIFI_NUM',
        'Tarjetas de credito bancarias vigentes titulares por institucion',
        'Monthly active primary bank credit cards by institution from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'count',
        date '2009-04-01',
        true
    ),
    (
        'bank_credit_card_active_cards_supplementary',
        'Operations Rate',
        'active_cards_supplementary',
        'SBIF_TCRED_BANC_VIGADIC_AGIFI_NUM',
        'Tarjetas de credito bancarias vigentes adicionales por institucion',
        'Monthly active supplementary bank credit cards by institution from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'count',
        date '2009-04-01',
        true
    ),
    (
        'bank_credit_card_cards_with_operations_primary',
        'Operations Rate',
        'cards_with_operations_primary',
        'SBIF_TCRED_BANC_COPETIT_AGIFI_NUM',
        'Tarjetas de credito bancarias titulares con operaciones por institucion',
        'Monthly primary bank credit cards with operations by institution from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'count',
        date '2009-04-01',
        true
    ),
    (
        'bank_credit_card_cards_with_operations_supplementary',
        'Operations Rate',
        'cards_with_operations_supplementary',
        'SBIF_TCRED_BANC_COPEADIC_AGIFI_NUM',
        'Tarjetas de credito bancarias adicionales con operaciones por institucion',
        'Monthly supplementary bank credit cards with operations by institution from CMF Cuadrosv2.',
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

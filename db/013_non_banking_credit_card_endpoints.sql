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
        'bank_credit_card_ops_non_banking_compras_transaction_count',
        'Compras No Bancarias',
        'transaction_count',
        'SBIF_TCRED_NBANC_OPER_AGIFI_MRC_NUM',
        'Operaciones con tarjetas de credito no bancarias por emisor y marca',
        'Monthly non-banking credit-card purchase transaction counts by institution and brand from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'count',
        date '2009-04-01',
        true
    ),
    (
        'bank_credit_card_ops_non_banking_compras_nominal_volume',
        'Compras No Bancarias',
        'nominal_volume',
        'SBIF_TCRED_NBANC_OPER_AGIFI_MRC_$',
        'Operaciones con tarjetas de credito no bancarias por emisor y marca',
        'Monthly non-banking credit-card purchase nominal volume by institution and brand from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'millions_clp',
        date '2009-04-01',
        true
    ),
    (
        'bank_credit_card_active_cards_non_banking',
        'Total Activation Rate',
        'active_cards_non_banking',
        'SBIF_TCRED_NBANC_VIG_AGIFI_MRC_NUM',
        'Tarjetas de credito no bancarias vigentes por emisor y marca',
        'Monthly active non-banking credit cards by institution and brand from CMF Cuadrosv2.',
        'https://best-sbif-api.azurewebsites.net/Cuadrosv2',
        'monthly',
        'count',
        date '2009-04-01',
        true
    ),
    (
        'bank_credit_card_cards_with_operations_non_banking',
        'Total Activation Rate',
        'cards_with_operations_non_banking',
        'SBIF_TCRED_NBANC_COPE_AGIFI_MRC_NUM',
        'Tarjetas de credito no bancarias con operaciones por emisor y marca',
        'Monthly non-banking credit cards with operations by institution and brand from CMF Cuadrosv2.',
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

create or replace view public.bank_credit_card_operations_rate_metrics as
select
    counts.institution_code,
    counts.institution_name,
    counts.period_month,
    counts.total_active_cards,
    counts.active_cards_primary,
    counts.active_cards_supplementary,
    counts.cards_with_operations_primary,
    counts.cards_with_operations_supplementary,
    counts.total_cards_with_operations,
    counts.operations_rate
from public.bank_credit_card_counts_curated as counts
where counts.dataset_code = 'bank_credit_card_counts';

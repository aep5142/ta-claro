begin;

alter table if exists public.cmf_datasets
    add column if not exists measure_kind text;

alter table if exists public.bank_credit_card_ops_raw
    drop constraint if exists bank_credit_card_ops_raw_dataset_code_fkey;

alter table if exists public.bank_credit_card_ops_curated
    drop constraint if exists bank_credit_card_ops_curated_dataset_code_fkey;

alter table if exists public.bank_credit_card_ops_curated
    drop constraint if exists bank_credit_card_ops_curated_source_dataset_code_fkey;

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
    is_active,
    created_at,
    updated_at
)
select
    bank_credit_card_ops_compras_transaction_count.dataset_code,
    source_rows.operation_type,
    bank_credit_card_ops_compras_transaction_count.measure_kind,
    bank_credit_card_ops_compras_transaction_count.source_tag,
    source_rows.source_nombre,
    bank_credit_card_ops_compras_transaction_count.source_description,
    source_rows.source_endpoint_base,
    source_rows.refresh_frequency,
    bank_credit_card_ops_compras_transaction_count.source_unit,
    source_rows.start_date,
    source_rows.is_active,
    source_rows.created_at,
    source_rows.updated_at
from public.cmf_datasets as source_rows
cross join (
    values
        ('Compras', 'bank_credit_card_ops_compras_transaction_count', 'transaction_count', 'SBIF_TCRED_BANC_COMP_AGIFI_NUM', 'Monthly bank credit card purchase transaction counts by institution from CMF Cuadrosv2.', 'count'),
        ('Compras', 'bank_credit_card_ops_compras_nominal_volume', 'nominal_volume', 'SBIF_TCRED_BANC_COMP_AGIFI_$', 'Monthly bank credit card purchase nominal volume by institution from CMF Cuadrosv2.', 'millions_clp'),
        ('Avance en Efectivo', 'bank_credit_card_ops_avance_en_efectivo_transaction_count', 'transaction_count', 'SBIF_TCRED_BANC_AVEF_AGIFI_NUM', 'Monthly bank credit card cash advance transaction counts by institution from CMF Cuadrosv2.', 'count'),
        ('Avance en Efectivo', 'bank_credit_card_ops_avance_en_efectivo_nominal_volume', 'nominal_volume', 'SBIF_TCRED_BANC_AVEF_AGIFI_$', 'Monthly bank credit card cash advance nominal volume by institution from CMF Cuadrosv2.', 'millions_clp'),
        ('Cargos por Servicio', 'bank_credit_card_ops_cargos_por_servicio_transaction_count', 'transaction_count', 'SBIF_TCRED_BANC_CSERV_AGIFI_NUM', 'Monthly bank credit card service charge transaction counts by institution from CMF Cuadrosv2.', 'count'),
        ('Cargos por Servicio', 'bank_credit_card_ops_cargos_por_servicio_nominal_volume', 'nominal_volume', 'SBIF_TCRED_BANC_CSERV_AGIFI_$', 'Monthly bank credit card service charge nominal volume by institution from CMF Cuadrosv2.', 'millions_clp')
) as bank_credit_card_ops_compras_transaction_count(
    operation_type,
    dataset_code,
    measure_kind,
    source_tag,
    source_description,
    source_unit
)
where source_rows.operation_type = bank_credit_card_ops_compras_transaction_count.operation_type
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
    updated_at = excluded.updated_at;

insert into public.cmf_dataset_sync_state (
    dataset_code,
    latest_source_month,
    latest_curated_month,
    last_successful_sync_at,
    last_attempted_sync_at,
    last_error,
    updated_at
)
select
    dataset_rows.dataset_code,
    sync_rows.latest_source_month,
    sync_rows.latest_curated_month,
    sync_rows.last_successful_sync_at,
    sync_rows.last_attempted_sync_at,
    sync_rows.last_error,
    sync_rows.updated_at
from public.cmf_dataset_sync_state as sync_rows
join (
    values
        ('bank_credit_card_ops_compras', 'bank_credit_card_ops_compras_transaction_count'),
        ('bank_credit_card_ops_compras', 'bank_credit_card_ops_compras_nominal_volume'),
        ('bank_credit_card_ops_avance_en_efectivo', 'bank_credit_card_ops_avance_en_efectivo_transaction_count'),
        ('bank_credit_card_ops_avance_en_efectivo', 'bank_credit_card_ops_avance_en_efectivo_nominal_volume'),
        ('bank_credit_card_ops_cargos_por_servicio', 'bank_credit_card_ops_cargos_por_servicio_transaction_count'),
        ('bank_credit_card_ops_cargos_por_servicio', 'bank_credit_card_ops_cargos_por_servicio_nominal_volume')
) as dataset_rows(old_dataset_code, dataset_code)
    on sync_rows.dataset_code = dataset_rows.old_dataset_code
on conflict (dataset_code) do update set
    latest_source_month = excluded.latest_source_month,
    latest_curated_month = excluded.latest_curated_month,
    last_successful_sync_at = excluded.last_successful_sync_at,
    last_attempted_sync_at = excluded.last_attempted_sync_at,
    last_error = excluded.last_error,
    updated_at = excluded.updated_at;

delete from public.cmf_dataset_sync_state
where dataset_code in (
    'bank_credit_card_ops_compras',
    'bank_credit_card_ops_avance_en_efectivo',
    'bank_credit_card_ops_cargos_por_servicio'
);

delete from public.cmf_datasets
where dataset_code in (
    'bank_credit_card_ops_compras',
    'bank_credit_card_ops_avance_en_efectivo',
    'bank_credit_card_ops_cargos_por_servicio'
);

alter table if exists public.cmf_datasets
    drop column if exists transaction_count_source_tag;

alter table if exists public.cmf_datasets
    drop column if exists nominal_volume_source_tag;

commit;

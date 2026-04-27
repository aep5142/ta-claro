-- Migrate credit-card ops orchestration onto the shared CMF registry/state tables.

begin;

alter table if exists public.cmf_datasets
    add column if not exists operation_type text;

alter table if exists public.cmf_datasets
    add column if not exists transaction_count_source_tag text;

alter table if exists public.cmf_datasets
    add column if not exists nominal_volume_source_tag text;

alter table if exists public.bank_credit_card_ops_raw
    drop constraint if exists bank_credit_card_ops_raw_dataset_code_fkey;

alter table if exists public.bank_credit_card_ops_curated
    drop constraint if exists bank_credit_card_ops_curated_dataset_code_fkey;

alter table if exists public.bank_credit_card_ops_curated
    drop constraint if exists bank_credit_card_ops_curated_source_dataset_code_fkey;

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
    is_active,
    created_at,
    updated_at
)
select
    dataset_code,
    operation_type,
    regexp_replace(transaction_count_source_tag, '_NUM$', '') as source_tag,
    transaction_count_source_tag,
    nominal_volume_source_tag,
    source_nombre,
    source_description,
    source_endpoint_base,
    refresh_frequency,
    'combined',
    start_date,
    is_active,
    created_at,
    updated_at
from public.bank_credit_card_ops_registry
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
    dataset_code,
    latest_source_month,
    latest_curated_month,
    last_successful_sync_at,
    last_attempted_sync_at,
    last_error,
    updated_at
from public.bank_credit_card_ops_sync_state
on conflict (dataset_code) do update set
    latest_source_month = excluded.latest_source_month,
    latest_curated_month = excluded.latest_curated_month,
    last_successful_sync_at = excluded.last_successful_sync_at,
    last_attempted_sync_at = excluded.last_attempted_sync_at,
    last_error = excluded.last_error,
    updated_at = excluded.updated_at;

alter table if exists public.bank_credit_card_ops_raw
    add constraint bank_credit_card_ops_raw_dataset_code_fkey
    foreign key (dataset_code) references public.cmf_datasets(dataset_code);

alter table if exists public.bank_credit_card_ops_curated
    add constraint bank_credit_card_ops_curated_dataset_code_fkey
    foreign key (dataset_code) references public.cmf_datasets(dataset_code);

alter table if exists public.bank_credit_card_ops_curated
    add constraint bank_credit_card_ops_curated_source_dataset_code_fkey
    foreign key (source_dataset_code) references public.cmf_datasets(dataset_code);

drop table if exists public.bank_credit_card_ops_registry cascade;
drop table if exists public.bank_credit_card_ops_sync_state cascade;

commit;

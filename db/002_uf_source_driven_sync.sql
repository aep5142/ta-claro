-- Revise UF sync state from monthly markers to source-driven singleton state.
-- This migration keeps UF state separate from CMF state.

alter table if exists public.uf_sync_runs
    rename column month_key to sync_key;

alter table if exists public.uf_sync_runs
    add column if not exists latest_source_uf_date date,
    add column if not exists latest_stored_uf_date date,
    add column if not exists rows_upserted integer not null default 0,
    add column if not exists last_error text;

delete from public.uf_sync_runs
where sync_key <> 'uf_values';

insert into public.uf_sync_runs (
    sync_key,
    latest_source_uf_date,
    latest_stored_uf_date,
    rows_upserted
) values (
    'uf_values',
    null,
    null,
    0
)
on conflict (sync_key) do nothing;

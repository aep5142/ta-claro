from pathlib import Path


def test_cmf_ops_foundation_sql_defines_unified_registry_and_tables():
    sql = Path("db/001_cmf_foundation.sql").read_text()

    assert "create table if not exists public.cmf_datasets" in sql
    assert "create table if not exists public.cmf_dataset_sync_state" in sql
    assert "create table if not exists public.bank_credit_card_ops_raw" in sql
    assert "create table if not exists public.bank_credit_card_ops_curated" in sql
    assert "bank_credit_card_ops_compras" in sql
    assert "bank_credit_card_ops_avance_en_efectivo" in sql
    assert "bank_credit_card_ops_cargos_por_servicio" in sql
    assert "bank_credit_card_ops_raw_period_idx" in sql
    assert "bank_credit_card_ops_curated_period_idx" in sql
    assert "bank_credit_card_ops_registry" not in sql
    assert "bank_credit_card_ops_sync_state" not in sql


def test_cmf_ops_views_sql_defines_combined_public_view():
    sql = Path("db/003_bank_credit_card_ops_views.sql").read_text()

    assert "create or replace view public.bank_credit_card_ops_metrics" in sql
    assert "nominal_volume_millions_clp" not in sql
    assert "average_ticket_clp_today" not in sql
    assert "nominal_volume_thousands_millions_clp" in sql
    assert "average_ticket_uf" in sql


def test_cmf_ops_cleanup_sql_drops_obsolete_purchase_views():
    sql = Path("db/004_drop_obsolete_credit_card_views.sql").read_text()

    assert "drop view if exists public.bank_credit_card_purchase;" in sql
    assert "drop view if exists public.bank_credit_card_purchases;" in sql
    assert "drop view if exists public.bank_credit_card_purchases_metrics;" in sql
    assert "drop table if exists public.bank_credit_card_purchase_metrics;" in sql


def test_cmf_ops_cleanup_sql_drops_obsolete_split_tables():
    sql = Path("db/005_drop_obsolete_credit_card_tables.sql").read_text()

    assert "drop table if exists public.bank_credit_card_transaction_count_raw;" in sql
    assert "drop table if exists public.bank_credit_card_transaction_count_curated;" in sql
    assert "drop table if exists public.bank_credit_card_purchase_volume_raw;" in sql
    assert "drop table if exists public.bank_credit_card_purchase_volume_curated;" in sql

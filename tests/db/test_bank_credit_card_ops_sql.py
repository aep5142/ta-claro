from pathlib import Path


def test_cmf_ops_foundation_sql_defines_unified_registry_and_tables():
    sql = Path("db/001_cmf_foundation.sql").read_text()

    assert "create table if not exists public.cmf_datasets" in sql
    assert "create table if not exists public.cmf_dataset_sync_state" in sql
    assert "create table if not exists public.bank_credit_card_ops_raw" in sql
    assert "create table if not exists public.bank_credit_card_ops_curated" in sql
    assert "measure_kind text" in sql
    assert "bank_credit_card_ops_compras_transaction_count" in sql
    assert "bank_credit_card_ops_compras_nominal_volume" in sql
    assert "bank_credit_card_ops_avance_en_efectivo_transaction_count" in sql
    assert "bank_credit_card_ops_avance_en_efectivo_nominal_volume" in sql
    assert "bank_credit_card_ops_cargos_por_servicio_transaction_count" in sql
    assert "bank_credit_card_ops_cargos_por_servicio_nominal_volume" in sql
    assert "bank_credit_card_ops_raw_period_idx" in sql
    assert "bank_credit_card_ops_curated_period_idx" in sql
    assert "transaction_count_source_tag" not in sql
    assert "nominal_volume_source_tag" not in sql


def test_cmf_ops_views_sql_defines_combined_public_view():
    sql = Path("db/003_bank_credit_card_ops_views.sql").read_text()

    assert "create or replace view public.bank_credit_card_ops_metrics" in sql
    assert "nominal_volume_millions_clp" not in sql
    assert "average_ticket_clp_today" not in sql
    assert "nominal_volume_thousands_millions_clp" in sql
    assert "average_ticket_uf" in sql


def test_credit_card_card_counts_sql_registers_new_dataset_tables():
    sql = Path("db/008_credit_card_card_counts.sql").read_text()

    assert "create table if not exists public.bank_credit_card_counts_raw" in sql
    assert "create table if not exists public.bank_credit_card_counts_curated" in sql
    assert "bank_credit_card_active_cards_primary" in sql
    assert "bank_credit_card_active_cards_supplementary" in sql
    assert "bank_credit_card_cards_with_operations_primary" in sql
    assert "bank_credit_card_cards_with_operations_supplementary" in sql
    assert "date '2009-04-01'" in sql


def test_credit_card_metrics_rollback_sql_defines_new_public_contract():
    sql = Path("db/009_credit_card_metrics_rollback.sql").read_text()

    assert "rename column nominal_volume_thousands_millions_clp to nominal_volume_millions_clp" in sql
    assert "alter view public.bank_credit_card_ops_metrics" in sql
    assert "create or replace view public.bank_credit_card_ops_metrics" in sql
    assert "nominal_volume_millions_clp" in sql
    assert "operations_per_active_card" in sql
    assert "source_dataset_code,\n    curated.updated_at,\n    curated.total_active_cards" in sql
    assert "create or replace view public.bank_credit_card_operations_rate_metrics" in sql
    assert "total_active_cards" in sql
    assert "active_cards_primary" in sql
    assert "active_cards_supplementary" in sql
    assert "total_cards_with_operations" in sql
    assert "market_share_percent" not in sql


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


def test_rename_operations_rate_sql_updates_card_count_operation_type():
    sql = Path("db/011_rename_operations_rate_to_total_activation_rate.sql").read_text()

    assert "update public.cmf_datasets" in sql
    assert "Total Activation Rate" in sql
    assert "where operation_type = 'Operations Rate'" in sql
    assert "bank_credit_card_active_cards_primary" in sql
    assert "bank_credit_card_active_cards_supplementary" in sql
    assert "bank_credit_card_cards_with_operations_primary" in sql
    assert "bank_credit_card_cards_with_operations_supplementary" in sql

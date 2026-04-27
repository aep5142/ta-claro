from pathlib import Path


def test_cmf_ops_foundation_sql_defines_unified_registry_and_tables():
    sql = Path("db/001_cmf_foundation.sql").read_text()

    assert "create table if not exists public.bank_credit_card_ops_registry" in sql
    assert "create table if not exists public.bank_credit_card_ops_sync_state" in sql
    assert "create table if not exists public.bank_credit_card_ops_raw" in sql
    assert "create table if not exists public.bank_credit_card_ops_curated" in sql
    assert "bank_credit_card_ops_compras" in sql
    assert "bank_credit_card_ops_avance_en_efectivo" in sql
    assert "bank_credit_card_ops_cargos_por_servicio" in sql
    assert "bank_credit_card_ops_raw_period_idx" in sql
    assert "bank_credit_card_ops_curated_period_idx" in sql


def test_cmf_ops_views_sql_defines_combined_public_view():
    sql = Path("db/003_bank_credit_card_ops_views.sql").read_text()

    assert "create or replace view public.bank_credit_card_ops_metrics" in sql
    assert "nominal_volume_thousands_millions_clp * 1000 as nominal_volume_millions_clp" in sql
    assert "average_ticket_uf * latest_uf.latest_uf_value as average_ticket_clp_today" in sql
    assert "latest_uf_value" not in sql.split("create or replace view public.bank_credit_card_ops_metrics", 1)[0]

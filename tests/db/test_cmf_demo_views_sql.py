from pathlib import Path


def test_cmf_cards_cleanup_sql_defines_renamed_tables_and_metrics_surface():
    sql = Path("db/004_cmf_cards_cleanup.sql").read_text()

    assert "rename to bank_credit_card_transaction_count_raw" in sql
    assert "rename to bank_credit_card_transaction_count_curated" in sql
    assert "rename to bank_credit_card_purchase_volume_raw" in sql
    assert "rename to bank_credit_card_purchase_volume_curated" in sql
    assert "rename column nominal_volume_clp to nominal_volume_millions_clp" in sql
    assert (
        "rename column nominal_volume_clp to nominal_volume_thousands_millions_clp"
        in sql
    )
    assert "create table if not exists public.bank_credit_card_purchase_metrics" in sql
    assert "average_ticket_uf" in sql
    assert "create or replace view public.bank_credit_card_purchase" in sql
    assert "average_ticket_clp_today" in sql
    assert "create or replace view public.cmf_card_monthly_metrics" not in sql
    assert "create or replace view public.cmf_latest_uf" not in sql
    assert "create or replace view public.cmf_card_monthly_demo_metrics" not in sql

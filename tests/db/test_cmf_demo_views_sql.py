from pathlib import Path


def test_cmf_demo_views_sql_defines_joined_monthly_demo_views():
    sql = Path("db/003_cmf_demo_views.sql").read_text()

    assert "create or replace view public.cmf_card_monthly_metrics" in sql
    assert "full outer join public.cmf_card_transaction_count_curated" in sql
    assert "volume.institution_code = transactions.institution_code" in sql
    assert "volume.period_month = transactions.period_month" in sql
    assert "create or replace view public.cmf_latest_uf" in sql
    assert "create or replace view public.cmf_card_monthly_demo_metrics" in sql
    assert "average_ticket_clp_today" in sql

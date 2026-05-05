from pathlib import Path


def test_debit_card_config_exposes_routes_and_supported_views():
    src = Path("front/lib/debit-card-config.ts").read_text()

    assert 'slug: "transactions"' in src
    assert 'slug: "atm-withdrawals"' in src
    assert 'slug: "total-activation-rate"' in src
    assert 'operation: "Debit Transactions"' in src
    assert 'operation: "ATM Withdrawals"' in src
    assert 'operation: "Total Activation Rate"' in src
    assert 'key: "supplementary-rate"' in src
    assert 'key: "primary-activation-rate"' not in src
    assert 'key: "supplementary-activation-rate"' not in src


def test_debit_supabase_queries_target_debit_views():
    src = Path("front/lib/supabase-debit-queries.ts").read_text()

    assert 'from("bank_debit_card_ops_metrics")' in src
    assert 'from("bank_debit_card_operation_metrics")' in src
    assert "METRICS_PAGE_SIZE = 1000" in src


def test_debit_routes_wire_to_operation_slug_model():
    page_src = Path("front/app/debit-cards/page.tsx").read_text()
    dynamic_src = Path("front/app/debit-cards/[operation]/page.tsx").read_text()

    assert "/debit-cards/transactions" in page_src
    assert "operationFromSlug" in dynamic_src

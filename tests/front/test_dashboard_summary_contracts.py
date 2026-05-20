from pathlib import Path


def test_card_dashboards_use_shared_volume_helper_for_summary_totals():
    credit_src = Path("front/components/credit-cards-dashboard.tsx").read_text()
    debit_src = Path("front/components/debit-cards-dashboard.tsx").read_text()
    prepaid_src = Path("front/components/prepaid-cards-dashboard.tsx").read_text()

    for src in (credit_src, debit_src, prepaid_src):
        assert 'getOperationMetricValue(row, "volume", activeUfValue)' in src
        assert "accumulator.volume += volumeValue ?? 0;" in src
        assert (
            'viewKey === "volume"\n                ? (getOperationMetricValue('
        ) in src

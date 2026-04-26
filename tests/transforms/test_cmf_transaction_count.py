from datetime import date
from decimal import Decimal

from data.models.bank_credit_card_operations import (
    BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET,
    CmfTransactionCountRawObservation,
)
from data.transforms.cmf_transaction_count import to_curated_transaction_count


def test_to_curated_transaction_count_keeps_approved_fields():
    raw_observations = [
        CmfTransactionCountRawObservation(
            dataset_code=BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET,
            source_series_id="101",
            source_codigo="SBIF_TCRED_BANC_COMP_AGIFI_001_NUM",
            source_nombre="Banco Uno",
            institution_code="001",
            institution_name="Banco Uno",
            period_month=date(2026, 4, 1),
            transaction_count=Decimal("1234"),
            source_payload={"Fecha": "2026-04-01", "Valor": "1.234"},
        )
    ]

    curated = to_curated_transaction_count(raw_observations)

    assert len(curated) == 1
    assert curated[0].institution_code == "001"
    assert curated[0].institution_name == "Banco Uno"
    assert curated[0].period_month == date(2026, 4, 1)
    assert curated[0].transaction_count == Decimal("1234")
    assert curated[0].source_dataset_code == BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET

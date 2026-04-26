from datetime import date
from decimal import Decimal

import pytest

from data.models.bank_credit_card_operations import (
    BANK_CREDIT_CARD_PURCHASE_VOLUME_DATASET,
    CmfPurchaseVolumeRawObservation,
)
from data.transforms.cmf_purchase_volume import (
    to_curated_purchase_volume,
    uf_conversion_date,
)


def _raw_observation():
    return CmfPurchaseVolumeRawObservation(
        dataset_code=BANK_CREDIT_CARD_PURCHASE_VOLUME_DATASET,
        source_series_id="301",
        source_codigo="SBIF_TCRED_BANC_COMP_AGIFI_001_$",
        source_nombre="Banco Uno",
        institution_code="001",
        institution_name="Banco Uno",
        period_month=date(2026, 4, 1),
        nominal_volume_clp=Decimal("1000000"),
        source_payload={"Fecha": "2026-04-01", "Valor": "1.000.000"},
    )


def test_uf_conversion_date_uses_15th_day_of_same_month():
    assert uf_conversion_date(date(2026, 4, 1)) == date(2026, 4, 15)


def test_to_curated_purchase_volume_enriches_with_uf_and_real_volume():
    curated = to_curated_purchase_volume(
        [_raw_observation()],
        uf_lookup=lambda uf_date: {
            date(2026, 4, 15): Decimal("40000"),
        }[uf_date],
    )

    assert len(curated) == 1
    assert curated[0].institution_code == "001"
    assert curated[0].period_month == date(2026, 4, 1)
    assert curated[0].nominal_volume_clp == Decimal("1000000")
    assert curated[0].uf_date_used == date(2026, 4, 15)
    assert curated[0].uf_value_used == Decimal("40000")
    assert curated[0].real_volume_uf == Decimal("25")
    assert curated[0].source_dataset_code == BANK_CREDIT_CARD_PURCHASE_VOLUME_DATASET


def test_to_curated_purchase_volume_propagates_missing_uf_failure():
    def missing_uf_lookup(_uf_date):
        raise ValueError("Missing UF value")

    with pytest.raises(ValueError, match="Missing UF value"):
        to_curated_purchase_volume([_raw_observation()], uf_lookup=missing_uf_lookup)

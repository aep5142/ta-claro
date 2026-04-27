from datetime import date
from decimal import Decimal

import pytest

from data.models.bank_credit_card_operations import (
    BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_DATASET,
    BANK_CREDIT_CARD_OPS_COMPRAS_DATASET,
    BankCreditCardOpsRawObservation,
)
from data.transforms.bank_credit_card_ops import (
    to_curated_bank_credit_card_ops,
    uf_conversion_date,
)


def _raw_observation():
    return BankCreditCardOpsRawObservation(
        operation_type="Compras",
        dataset_code=BANK_CREDIT_CARD_OPS_COMPRAS_DATASET,
        source_series_id="301",
        source_codigo="SBIF_TCRED_BANC_COMP_AGIFI_BICE_$",
        source_nombre="Banco BICE",
        institution_code="BICE",
        institution_name="Banco BICE",
        period_month=date(2026, 4, 1),
        transaction_count=Decimal("2500"),
        nominal_volume_millions_clp=Decimal("120507338"),
        source_payload={
            "transaction_count": {"Fecha": "2026-04-01", "Valor": "2.500"},
            "nominal_volume_millions_clp": {
                "Fecha": "2026-04-01",
                "Valor": "120.507.338",
            },
        },
    )


def test_uf_conversion_date_uses_15th_day_of_same_month():
    assert uf_conversion_date(date(2026, 4, 1)) == date(2026, 4, 15)


def test_to_curated_bank_credit_card_ops_enriches_with_uf_and_average_ticket():
    curated = to_curated_bank_credit_card_ops(
        [_raw_observation()],
        uf_lookup=lambda uf_date: {date(2026, 4, 15): Decimal("40000")}[uf_date],
    )

    assert len(curated) == 1
    assert curated[0].operation_type == "Compras"
    assert curated[0].institution_code == "BICE"
    assert curated[0].period_month == date(2026, 4, 1)
    assert curated[0].nominal_volume_thousands_millions_clp == Decimal("120507.338")
    assert curated[0].uf_date_used == date(2026, 4, 15)
    assert curated[0].uf_value_used == Decimal("40000")
    assert curated[0].real_value_uf == Decimal("3.01268345")
    assert curated[0].average_ticket_uf == Decimal("0.00120507338")
    assert curated[0].source_dataset_code == BANK_CREDIT_CARD_OPS_COMPRAS_DATASET


def test_to_curated_bank_credit_card_ops_propagates_missing_uf_failure():
    def missing_uf_lookup(_uf_date):
        raise ValueError("Missing UF value")

    with pytest.raises(ValueError, match="Missing UF value"):
        to_curated_bank_credit_card_ops([_raw_observation()], uf_lookup=missing_uf_lookup)


def test_to_curated_bank_credit_card_ops_keeps_operation_metadata():
    raw_observation = BankCreditCardOpsRawObservation(
        operation_type="Avance en Efectivo",
        dataset_code=BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_DATASET,
        source_series_id="102",
        source_codigo="SBIF_TCRED_BANC_AVEF_AGIFI_BICE_$",
        source_nombre="Banco BICE",
        institution_code="BICE",
        institution_name="Banco BICE",
        period_month=date(2026, 5, 1),
        transaction_count=Decimal("2000"),
        nominal_volume_millions_clp=Decimal("60000000"),
        source_payload={"transaction_count": {}, "nominal_volume_millions_clp": {}},
    )

    curated = to_curated_bank_credit_card_ops(
        [raw_observation],
        uf_lookup=lambda _uf_date: Decimal("50000"),
    )

    assert curated[0].operation_type == "Avance en Efectivo"
    assert curated[0].dataset_code == BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_DATASET

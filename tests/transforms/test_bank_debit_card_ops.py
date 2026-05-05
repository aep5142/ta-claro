from datetime import date
from decimal import Decimal

from data.models.bank_debit_card_operations import (
    BANK_DEBIT_CARD_ACTIVE_CARDS_PRIMARY_ATM_ONLY_DATASET,
    BANK_DEBIT_CARD_ACTIVE_CARDS_PRIMARY_DEBIT_DATASET,
    BANK_DEBIT_CARD_ACTIVE_CARDS_SUPPLEMENTARY_ATM_ONLY_DATASET,
    BANK_DEBIT_CARD_ACTIVE_CARDS_SUPPLEMENTARY_DEBIT_DATASET,
    BANK_DEBIT_CARD_CARDS_WITH_OPERATIONS_ATM_ONLY_DATASET,
    BANK_DEBIT_CARD_CARDS_WITH_OPERATIONS_DEBIT_DATASET,
    BANK_DEBIT_CARD_COUNTS_DATASET,
    BANK_DEBIT_CARD_OPS_ATM_WITHDRAWALS_DATASET,
    BANK_DEBIT_CARD_OPS_DEBIT_TRANSACTIONS_DATASET,
    BankDebitCardCountRawObservation,
    BankDebitCardOpsRawObservation,
)
from data.transforms.bank_debit_card_ops import (
    to_curated_bank_debit_card_counts,
    to_curated_bank_debit_card_ops,
    uf_conversion_date,
)


def _raw_observation():
    return BankDebitCardOpsRawObservation(
        operation_type="Debit Transactions",
        dataset_code=BANK_DEBIT_CARD_OPS_DEBIT_TRANSACTIONS_DATASET,
        source_series_id="301",
        source_codigo="SBIF_TDEB_TATM_OPER_TXDEB_AGIFI_BICE_NUM",
        source_nombre="Banco BICE",
        institution_code="BICE",
        institution_name="Banco BICE",
        period_month=date(2026, 4, 1),
        transaction_count=Decimal("2500"),
        nominal_volume_millions_clp=Decimal("120507.338"),
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


def test_to_curated_bank_debit_card_ops_enriches_with_uf_and_average_ticket():
    curated = to_curated_bank_debit_card_ops(
        [_raw_observation()],
        uf_lookup=lambda uf_date: {date(2026, 4, 15): Decimal("40000")}[uf_date],
    )

    assert len(curated) == 1
    assert curated[0].operation_type == "Debit Transactions"
    assert curated[0].dataset_code == BANK_DEBIT_CARD_OPS_DEBIT_TRANSACTIONS_DATASET
    assert curated[0].institution_code == "BICE"
    assert curated[0].nominal_volume_millions_clp == Decimal("120507.338")
    assert curated[0].uf_value_used == Decimal("40000")
    assert curated[0].real_value_uf == Decimal("3.01268345")
    assert curated[0].average_ticket_uf == Decimal("1205.07338")
    assert curated[0].total_active_cards is None
    assert curated[0].operations_per_active_card is None


def test_to_curated_bank_debit_card_ops_adds_operations_per_active_card():
    curated = to_curated_bank_debit_card_ops(
        [_raw_observation()],
        uf_lookup=lambda _uf_date: Decimal("40000"),
        active_cards_lookup=lambda institution_code, period_month: Decimal("500")
        if institution_code == "BICE" and period_month == date(2026, 4, 1)
        else None,
    )

    assert curated[0].total_active_cards == Decimal("500")
    assert curated[0].operations_per_active_card == Decimal("5")


def test_to_curated_bank_debit_card_ops_keeps_atm_withdrawal_metadata():
    raw_observation = BankDebitCardOpsRawObservation(
        operation_type="ATM Withdrawals",
        dataset_code=BANK_DEBIT_CARD_OPS_ATM_WITHDRAWALS_DATASET,
        source_series_id="102",
        source_codigo="SBIF_TDEB_TATM_OPER_GIR_AGIFI_BICE_NUM",
        source_nombre="Banco BICE",
        institution_code="BICE",
        institution_name="Banco BICE",
        period_month=date(2026, 5, 1),
        transaction_count=Decimal("2000"),
        nominal_volume_millions_clp=Decimal("60000"),
        source_payload={"transaction_count": {}, "nominal_volume_millions_clp": {}},
    )

    curated = to_curated_bank_debit_card_ops(
        [raw_observation],
        uf_lookup=lambda _uf_date: Decimal("50000"),
    )

    assert curated[0].operation_type == "ATM Withdrawals"
    assert curated[0].dataset_code == BANK_DEBIT_CARD_OPS_ATM_WITHDRAWALS_DATASET


def test_to_curated_bank_debit_card_counts_combines_debit_and_atm_only_counts():
    curated = to_curated_bank_debit_card_counts(
        [
            BankDebitCardCountRawObservation(
                dataset_code=BANK_DEBIT_CARD_ACTIVE_CARDS_PRIMARY_DEBIT_DATASET,
                source_series_id="101",
                source_codigo="SBIF_TDEB_VIGTIT_AGIFI_BICE_NUM",
                source_nombre="Banco BICE",
                institution_code="BICE",
                institution_name="Banco BICE",
                period_month=date(2026, 4, 1),
                card_count=Decimal("100"),
                source_payload={},
            ),
            BankDebitCardCountRawObservation(
                dataset_code=BANK_DEBIT_CARD_ACTIVE_CARDS_PRIMARY_ATM_ONLY_DATASET,
                source_series_id="102",
                source_codigo="SBIF_TATM_VIGTIT_AGIFI_BICE_NUM",
                source_nombre="Banco BICE",
                institution_code="BICE",
                institution_name="Banco BICE",
                period_month=date(2026, 4, 1),
                card_count=Decimal("25"),
                source_payload={},
            ),
            BankDebitCardCountRawObservation(
                dataset_code=BANK_DEBIT_CARD_ACTIVE_CARDS_SUPPLEMENTARY_DEBIT_DATASET,
                source_series_id="103",
                source_codigo="SBIF_TDEB_VIGADIC_AGIFI_BICE_NUM",
                source_nombre="Banco BICE",
                institution_code="BICE",
                institution_name="Banco BICE",
                period_month=date(2026, 4, 1),
                card_count=Decimal("10"),
                source_payload={},
            ),
            BankDebitCardCountRawObservation(
                dataset_code=BANK_DEBIT_CARD_ACTIVE_CARDS_SUPPLEMENTARY_ATM_ONLY_DATASET,
                source_series_id="104",
                source_codigo="SBIF_TATM_VIGADIC_AGIFI_BICE_NUM",
                source_nombre="Banco BICE",
                institution_code="BICE",
                institution_name="Banco BICE",
                period_month=date(2026, 4, 1),
                card_count=Decimal("5"),
                source_payload={},
            ),
            BankDebitCardCountRawObservation(
                dataset_code=BANK_DEBIT_CARD_CARDS_WITH_OPERATIONS_DEBIT_DATASET,
                source_series_id="105",
                source_codigo="SBIF_TDEB_COPE_AGIFI_BICE_NUM",
                source_nombre="Banco BICE",
                institution_code="BICE",
                institution_name="Banco BICE",
                period_month=date(2026, 4, 1),
                card_count=Decimal("80"),
                source_payload={},
            ),
            BankDebitCardCountRawObservation(
                dataset_code=BANK_DEBIT_CARD_CARDS_WITH_OPERATIONS_ATM_ONLY_DATASET,
                source_series_id="106",
                source_codigo="SBIF_TATM_COPE_AGIFI_BICE_NUM",
                source_nombre="Banco BICE",
                institution_code="BICE",
                institution_name="Banco BICE",
                period_month=date(2026, 4, 1),
                card_count=Decimal("15"),
                source_payload={},
            ),
        ]
    )

    assert len(curated) == 1
    assert curated[0].dataset_code == BANK_DEBIT_CARD_COUNTS_DATASET
    assert curated[0].active_cards_primary == Decimal("125")
    assert curated[0].active_cards_supplementary == Decimal("15")
    assert curated[0].total_active_cards == Decimal("140")
    assert curated[0].total_cards_with_operations == Decimal("95")
    assert curated[0].operations_rate == Decimal("0.6785714285714285714285714286")

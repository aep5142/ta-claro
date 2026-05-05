from datetime import date
from decimal import Decimal
from typing import Callable

from data.models.bank_debit_card_operations import (
    BANK_DEBIT_CARD_ACTIVE_CARDS_PRIMARY_ATM_ONLY_DATASET,
    BANK_DEBIT_CARD_ACTIVE_CARDS_PRIMARY_DEBIT_DATASET,
    BANK_DEBIT_CARD_ACTIVE_CARDS_SUPPLEMENTARY_ATM_ONLY_DATASET,
    BANK_DEBIT_CARD_ACTIVE_CARDS_SUPPLEMENTARY_DEBIT_DATASET,
    BANK_DEBIT_CARD_CARDS_WITH_OPERATIONS_ATM_ONLY_DATASET,
    BANK_DEBIT_CARD_CARDS_WITH_OPERATIONS_DEBIT_DATASET,
    BANK_DEBIT_CARD_COUNTS_DATASET,
    BankDebitCardCountsCuratedObservation,
    BankDebitCardCountRawObservation,
    BankDebitCardOpsCuratedObservation,
    BankDebitCardOpsRawObservation,
)


def uf_conversion_date(period_month: date) -> date:
    return period_month.replace(day=15)


def to_curated_bank_debit_card_ops(
    raw_observations: list[BankDebitCardOpsRawObservation],
    *,
    uf_lookup: Callable[[date], Decimal],
    active_cards_lookup: Callable[[str, date], Decimal | None] | None = None,
) -> list[BankDebitCardOpsCuratedObservation]:
    curated_observations: list[BankDebitCardOpsCuratedObservation] = []

    for observation in raw_observations:
        uf_date = uf_conversion_date(observation.period_month)
        uf_value = uf_lookup(uf_date)
        nominal_volume_millions_clp = observation.nominal_volume_millions_clp
        real_value_uf = nominal_volume_millions_clp / uf_value
        average_ticket_uf = (
            real_value_uf / observation.transaction_count
        ) * Decimal("1000000")
        total_active_cards = (
            active_cards_lookup(observation.institution_code, observation.period_month)
            if active_cards_lookup is not None
            else None
        )
        operations_per_active_card = None
        if total_active_cards not in (None, Decimal("0")):
            operations_per_active_card = observation.transaction_count / total_active_cards

        curated_observations.append(
            BankDebitCardOpsCuratedObservation(
                operation_type=observation.operation_type,
                dataset_code=observation.dataset_code,
                institution_code=observation.institution_code,
                institution_name=observation.institution_name,
                period_month=observation.period_month,
                transaction_count=observation.transaction_count,
                nominal_volume_millions_clp=nominal_volume_millions_clp,
                uf_date_used=uf_date,
                uf_value_used=uf_value,
                real_value_uf=real_value_uf,
                average_ticket_uf=average_ticket_uf,
                total_active_cards=total_active_cards,
                operations_per_active_card=operations_per_active_card,
                source_dataset_code=observation.dataset_code,
            )
        )

    return sorted(
        curated_observations,
        key=lambda observation: (
            observation.operation_type,
            observation.institution_code,
            observation.period_month,
        ),
    )


def to_curated_bank_debit_card_counts(
    raw_observations: list[BankDebitCardCountRawObservation],
) -> list[BankDebitCardCountsCuratedObservation]:
    counts_by_key: dict[tuple[str, date], dict[str, Decimal | str]] = {}

    for observation in raw_observations:
        key = (observation.institution_code, observation.period_month)
        row = counts_by_key.setdefault(
            key,
            {
                "institution_name": observation.institution_name,
                "active_cards_primary": Decimal("0"),
                "active_cards_supplementary": Decimal("0"),
                "total_cards_with_operations": Decimal("0"),
            },
        )
        row["institution_name"] = observation.institution_name

        if observation.dataset_code in {
            BANK_DEBIT_CARD_ACTIVE_CARDS_PRIMARY_DEBIT_DATASET,
            BANK_DEBIT_CARD_ACTIVE_CARDS_PRIMARY_ATM_ONLY_DATASET,
        }:
            row["active_cards_primary"] = row["active_cards_primary"] + observation.card_count
        elif observation.dataset_code in {
            BANK_DEBIT_CARD_ACTIVE_CARDS_SUPPLEMENTARY_DEBIT_DATASET,
            BANK_DEBIT_CARD_ACTIVE_CARDS_SUPPLEMENTARY_ATM_ONLY_DATASET,
        }:
            row["active_cards_supplementary"] = (
                row["active_cards_supplementary"] + observation.card_count
            )
        elif observation.dataset_code in {
            BANK_DEBIT_CARD_CARDS_WITH_OPERATIONS_DEBIT_DATASET,
            BANK_DEBIT_CARD_CARDS_WITH_OPERATIONS_ATM_ONLY_DATASET,
        }:
            row["total_cards_with_operations"] = (
                row["total_cards_with_operations"] + observation.card_count
            )

    curated: list[BankDebitCardCountsCuratedObservation] = []
    for (institution_code, period_month), row in sorted(counts_by_key.items()):
        active_cards_primary = row["active_cards_primary"]
        active_cards_supplementary = row["active_cards_supplementary"]
        total_active_cards = active_cards_primary + active_cards_supplementary
        total_cards_with_operations = row["total_cards_with_operations"]
        operations_rate = None
        if total_active_cards != Decimal("0"):
            operations_rate = total_cards_with_operations / total_active_cards

        curated.append(
            BankDebitCardCountsCuratedObservation(
                dataset_code=BANK_DEBIT_CARD_COUNTS_DATASET,
                institution_code=institution_code,
                institution_name=str(row["institution_name"]),
                period_month=period_month,
                active_cards_primary=active_cards_primary,
                active_cards_supplementary=active_cards_supplementary,
                total_active_cards=total_active_cards,
                total_cards_with_operations=total_cards_with_operations,
                operations_rate=operations_rate,
            )
        )

    return curated

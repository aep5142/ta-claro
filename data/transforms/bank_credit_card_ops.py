from datetime import date
from decimal import Decimal
from typing import Callable

from data.models.bank_credit_card_operations import (
    BankCreditCardOpsCuratedObservation,
    BankCreditCardOpsRawObservation,
)


def uf_conversion_date(period_month: date) -> date:
    return period_month.replace(day=15)


def to_curated_bank_credit_card_ops(
    raw_observations: list[BankCreditCardOpsRawObservation],
    *,
    uf_lookup: Callable[[date], Decimal],
) -> list[BankCreditCardOpsCuratedObservation]:
    curated_observations: list[BankCreditCardOpsCuratedObservation] = []

    for observation in raw_observations:
        uf_date = uf_conversion_date(observation.period_month)
        uf_value = uf_lookup(uf_date)
        nominal_volume_thousands_millions_clp = (
            observation.nominal_volume_millions_clp / Decimal("1000")
        )
        real_value_uf = nominal_volume_thousands_millions_clp / uf_value
        average_ticket_uf = real_value_uf / observation.transaction_count
        curated_observations.append(
            BankCreditCardOpsCuratedObservation(
                operation_type=observation.operation_type,
                dataset_code=observation.dataset_code,
                institution_code=observation.institution_code,
                institution_name=observation.institution_name,
                period_month=observation.period_month,
                transaction_count=observation.transaction_count,
                nominal_volume_thousands_millions_clp=nominal_volume_thousands_millions_clp,
                uf_date_used=uf_date,
                uf_value_used=uf_value,
                real_value_uf=real_value_uf,
                average_ticket_uf=average_ticket_uf,
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

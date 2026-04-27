from datetime import date
from decimal import Decimal
from typing import Callable

from data.models.bank_credit_card_operations import (
    BANK_CREDIT_CARD_PURCHASE_VOLUME_DATASET,
    CmfPurchaseVolumeCuratedObservation,
    CmfPurchaseVolumeRawObservation,
)


def uf_conversion_date(period_month: date) -> date:
    return period_month.replace(day=15)


def to_curated_purchase_volume(
    raw_observations: list[CmfPurchaseVolumeRawObservation],
    *,
    uf_lookup: Callable[[date], Decimal],
) -> list[CmfPurchaseVolumeCuratedObservation]:
    curated_observations: list[CmfPurchaseVolumeCuratedObservation] = []

    for observation in raw_observations:
        uf_date = uf_conversion_date(observation.period_month)
        uf_value = uf_lookup(uf_date)
        nominal_volume_thousands_millions_clp = (
            observation.nominal_volume_millions_clp / Decimal("1000")
        )
        curated_observations.append(
            CmfPurchaseVolumeCuratedObservation(
                institution_code=observation.institution_code,
                institution_name=observation.institution_name,
                period_month=observation.period_month,
                nominal_volume_thousands_millions_clp=nominal_volume_thousands_millions_clp,
                uf_date_used=uf_date,
                uf_value_used=uf_value,
                real_volume_uf=(
                    observation.nominal_volume_millions_clp * Decimal("1000000") / uf_value
                ),
                source_dataset_code=BANK_CREDIT_CARD_PURCHASE_VOLUME_DATASET,
            )
        )

    return curated_observations

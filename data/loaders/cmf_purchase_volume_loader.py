from datetime import date
from decimal import Decimal

from data.models.cmf_cards import (
    CmfPurchaseVolumeCuratedObservation,
    CmfPurchaseVolumeRawObservation,
)


def latest_curated_purchase_volume_month(sb) -> date | None:
    response = (
        sb.table("cmf_card_purchase_volume_curated")
        .select("period_month")
        .order("period_month", desc=True)
        .limit(1)
        .execute()
    )

    if not response.data:
        return None

    return date.fromisoformat(response.data[0]["period_month"])


def get_uf_value_for_date(sb, uf_date: date) -> Decimal:
    response = (
        sb.table("uf_values")
        .select("value")
        .eq("uf_date", uf_date.isoformat())
        .limit(1)
        .execute()
    )

    if not response.data:
        raise ValueError(f"Missing UF value for {uf_date.isoformat()}")

    return Decimal(str(response.data[0]["value"]))


def upsert_purchase_volume_raw(
    sb,
    observations: list[CmfPurchaseVolumeRawObservation],
):
    if not observations:
        return None

    return (
        sb.table("cmf_card_purchase_volume_raw")
        .upsert(
            [observation.to_row() for observation in observations],
            on_conflict="dataset_code,source_codigo,period_month",
        )
        .execute()
    )


def upsert_purchase_volume_curated(
    sb,
    observations: list[CmfPurchaseVolumeCuratedObservation],
):
    if not observations:
        return None

    return (
        sb.table("cmf_card_purchase_volume_curated")
        .upsert(
            [observation.to_row() for observation in observations],
            on_conflict="institution_code,period_month",
        )
        .execute()
    )

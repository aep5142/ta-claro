from datetime import date

from data.models.cmf_cards import (
    CmfTransactionCountCuratedObservation,
    CmfTransactionCountRawObservation,
)


def latest_curated_transaction_count_month(sb) -> date | None:
    response = (
        sb.table("cmf_card_transaction_count_curated")
        .select("period_month")
        .order("period_month", desc=True)
        .limit(1)
        .execute()
    )

    if not response.data:
        return None

    return date.fromisoformat(response.data[0]["period_month"])


def upsert_transaction_count_raw(
    sb,
    observations: list[CmfTransactionCountRawObservation],
):
    if not observations:
        return None

    return (
        sb.table("cmf_card_transaction_count_raw")
        .upsert(
            [observation.to_row() for observation in observations],
            on_conflict="dataset_code,source_codigo,period_month",
        )
        .execute()
    )


def upsert_transaction_count_curated(
    sb,
    observations: list[CmfTransactionCountCuratedObservation],
):
    if not observations:
        return None

    return (
        sb.table("cmf_card_transaction_count_curated")
        .upsert(
            [observation.to_row() for observation in observations],
            on_conflict="institution_code,period_month",
        )
        .execute()
    )

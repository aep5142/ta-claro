from datetime import date

from data.models.bank_credit_card_operations import (
    BANK_CREDIT_CARD_TRANSACTION_COUNT_CURATED_TABLE,
    BANK_CREDIT_CARD_TRANSACTION_COUNT_RAW_TABLE,
    CmfTransactionCountCuratedObservation,
    CmfTransactionCountRawObservation,
)


def latest_curated_transaction_count_month(sb) -> date | None:
    response = (
        sb.table(BANK_CREDIT_CARD_TRANSACTION_COUNT_CURATED_TABLE)
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
        sb.table(BANK_CREDIT_CARD_TRANSACTION_COUNT_RAW_TABLE)
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
        sb.table(BANK_CREDIT_CARD_TRANSACTION_COUNT_CURATED_TABLE)
        .upsert(
            [observation.to_row() for observation in observations],
            on_conflict="institution_code,period_month",
        )
        .execute()
    )

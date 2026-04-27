from datetime import date

from shared.time import now_santiago

from data.models.bank_credit_card_operations import BANK_CREDIT_CARD_OPS_SYNC_STATE_TABLE


def get_latest_state_source_month(sb, dataset_code: str) -> date | None:
    response = (
        sb.table(BANK_CREDIT_CARD_OPS_SYNC_STATE_TABLE)
        .select("latest_source_month")
        .eq("dataset_code", dataset_code)
        .limit(1)
        .execute()
    )

    if not response.data or not response.data[0].get("latest_source_month"):
        return None

    return date.fromisoformat(response.data[0]["latest_source_month"])


def record_sync_attempt(sb, dataset_code: str):
    return (
        sb.table(BANK_CREDIT_CARD_OPS_SYNC_STATE_TABLE)
        .upsert(
            {
                "dataset_code": dataset_code,
                "last_attempted_sync_at": now_santiago().isoformat(),
            },
            on_conflict="dataset_code",
        )
        .execute()
    )


def record_sync_success(
    sb,
    *,
    dataset_code: str,
    latest_source_month: date,
    latest_curated_month: date,
):
    return (
        sb.table(BANK_CREDIT_CARD_OPS_SYNC_STATE_TABLE)
        .upsert(
            {
                "dataset_code": dataset_code,
                "latest_source_month": latest_source_month.isoformat(),
                "latest_curated_month": latest_curated_month.isoformat(),
                "last_successful_sync_at": now_santiago().isoformat(),
                "last_error": None,
                "updated_at": now_santiago().isoformat(),
            },
            on_conflict="dataset_code",
        )
        .execute()
    )


def record_sync_failure(sb, *, dataset_code: str, error: Exception):
    return (
        sb.table(BANK_CREDIT_CARD_OPS_SYNC_STATE_TABLE)
        .upsert(
            {
                "dataset_code": dataset_code,
                "last_error": f"{type(error).__name__}: {error}",
                "updated_at": now_santiago().isoformat(),
            },
            on_conflict="dataset_code",
        )
        .execute()
    )

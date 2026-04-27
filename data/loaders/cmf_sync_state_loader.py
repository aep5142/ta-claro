from datetime import date, datetime

from shared.time import now_santiago


def get_cmf_sync_state(sb, dataset_code: str) -> dict | None:
    response = (
        sb.table("cmf_dataset_sync_state")
        .select("latest_source_month,latest_curated_month")
        .eq("dataset_code", dataset_code)
        .limit(1)
        .execute()
    )

    if not response.data:
        return None

    return response.data[0]


def get_latest_state_source_month(sb, dataset_code: str) -> date | None:
    state = get_cmf_sync_state(sb, dataset_code)
    if not state or not state.get("latest_source_month"):
        return None

    return date.fromisoformat(state["latest_source_month"])


def record_cmf_sync_attempt(sb, dataset_code: str):
    return (
        sb.table("cmf_dataset_sync_state")
        .upsert(
            {
                "dataset_code": dataset_code,
                "last_attempted_sync_at": now_santiago().isoformat(),
            },
            on_conflict="dataset_code",
        )
        .execute()
    )


def record_cmf_sync_success(
    sb,
    *,
    dataset_code: str,
    latest_source_month: date,
    latest_curated_month: date,
):
    return (
        sb.table("cmf_dataset_sync_state")
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


def record_cmf_sync_failure(sb, *, dataset_code: str, error: Exception):
    return (
        sb.table("cmf_dataset_sync_state")
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

from datetime import date

from shared.time import now_santiago

from data.models.uf import UfValue

UF_SYNC_KEY = "uf_values"


def latest_stored_uf_date(sb) -> date | None:
    response = (
        sb.table("uf_values")
        .select("uf_date")
        .order("uf_date", desc=True)
        .limit(1)
        .execute()
    )

    if not response.data:
        return None

    return date.fromisoformat(response.data[0]["uf_date"])


def new_uf_values(source_values: list[UfValue], latest_stored_date: date | None) -> list[UfValue]:
    if latest_stored_date is None:
        return source_values

    return [value for value in source_values if value.uf_date > latest_stored_date]


def upsert_uf_values(sb, values: list[UfValue]):
    if not values:
        return None

    return (
        sb.table("uf_values")
        .upsert([value.to_row() for value in values], on_conflict="uf_date")
        .execute()
    )


def record_uf_sync_success(
    sb,
    *,
    latest_source_date: date,
    latest_stored_date: date | None,
    rows_upserted: int,
):
    return (
        sb.table("uf_sync_runs")
        .upsert(
            {
                "sync_key": UF_SYNC_KEY,
                "latest_source_uf_date": latest_source_date.isoformat(),
                "latest_stored_uf_date": (
                    latest_stored_date.isoformat() if latest_stored_date else None
                ),
                "rows_upserted": rows_upserted,
                "synced_at": now_santiago().isoformat(),
                "last_error": None,
            },
            on_conflict="sync_key",
        )
        .execute()
    )


def record_uf_sync_failure(sb, error: Exception):
    return (
        sb.table("uf_sync_runs")
        .upsert(
            {
                "sync_key": UF_SYNC_KEY,
                "last_error": f"{type(error).__name__}: {error}",
                "synced_at": now_santiago().isoformat(),
            },
            on_conflict="sync_key",
        )
        .execute()
    )

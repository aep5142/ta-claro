from dataclasses import dataclass
from datetime import date

from data.loaders.cmf_transaction_count_loader import (
    latest_curated_transaction_count_month,
    upsert_transaction_count_curated,
    upsert_transaction_count_raw,
)
from data.sources.cmf_cards import fetch_transaction_count_observations
from data.transforms.cmf_transaction_count import to_curated_transaction_count


@dataclass(frozen=True)
class TransactionCountConfig:
    endpoint_base: str = "https://best-sbif-api.azurewebsites.net/Cuadrosv2"


async def sync_transaction_count_once(
    client,
    sb,
    *,
    config: TransactionCountConfig,
    run_date: date,
) -> int:
    raw_observations = await fetch_transaction_count_observations(
        client,
        endpoint_base=config.endpoint_base,
        fecha_fin=run_date,
    )
    latest_curated_month = latest_curated_transaction_count_month(sb)
    new_raw_observations = [
        observation
        for observation in raw_observations
        if latest_curated_month is None or observation.period_month > latest_curated_month
    ]

    if not new_raw_observations:
        return 0

    curated_observations = to_curated_transaction_count(new_raw_observations)
    upsert_transaction_count_raw(sb, new_raw_observations)
    upsert_transaction_count_curated(sb, curated_observations)
    return len(curated_observations)

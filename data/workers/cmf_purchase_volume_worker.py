from dataclasses import dataclass
from datetime import date

from data.loaders.cmf_purchase_volume_loader import (
    get_uf_value_for_date,
    latest_curated_purchase_volume_month,
    upsert_purchase_volume_curated,
    upsert_purchase_volume_raw,
)
from data.sources.bank_credit_card_operations import fetch_purchase_volume_observations
from data.transforms.cmf_purchase_volume import to_curated_purchase_volume


@dataclass(frozen=True)
class PurchaseVolumeConfig:
    endpoint_base: str = "https://best-sbif-api.azurewebsites.net/Cuadrosv2"


async def sync_purchase_volume_once(
    client,
    sb,
    *,
    config: PurchaseVolumeConfig,
    run_date: date,
) -> int:
    raw_observations = await fetch_purchase_volume_observations(
        client,
        endpoint_base=config.endpoint_base,
        fecha_fin=run_date,
    )
    latest_curated_month = latest_curated_purchase_volume_month(sb)
    new_raw_observations = [
        observation
        for observation in raw_observations
        if latest_curated_month is None or observation.period_month > latest_curated_month
    ]

    if not new_raw_observations:
        return 0

    curated_observations = to_curated_purchase_volume(
        new_raw_observations,
        uf_lookup=lambda uf_date: get_uf_value_for_date(sb, uf_date),
    )
    upsert_purchase_volume_raw(sb, new_raw_observations)
    upsert_purchase_volume_curated(sb, curated_observations)
    return len(curated_observations)

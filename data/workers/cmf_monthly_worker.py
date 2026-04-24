import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import date
from typing import Awaitable, Callable

import httpx
from dotenv import load_dotenv
from supabase import create_client

from data.loaders.cmf_sync_state_loader import (
    get_latest_state_source_month,
    record_cmf_sync_attempt,
    record_cmf_sync_failure,
    record_cmf_sync_success,
)
from data.models.cmf_cards import (
    BANK_CREDIT_CARD_PURCHASE_VOLUME_DATASET,
    BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET,
    CmfPurchaseVolumeRawObservation,
    CmfTransactionCountRawObservation,
)
from data.sources.cmf_cards import (
    fetch_purchase_volume_observations,
    fetch_transaction_count_observations,
)
from data.workers.cmf_purchase_volume_worker import (
    PurchaseVolumeConfig,
    sync_purchase_volume_once,
)
from data.workers.cmf_transaction_count_worker import (
    TransactionCountConfig,
    sync_transaction_count_once,
)

DAILY_INTERVAL_S = 24 * 60 * 60
DEFAULT_CMF_ENDPOINT_BASE = "https://best-sbif-api.azurewebsites.net/Cuadrosv2"

log = logging.getLogger("cmf-monthly-worker")


RawObservations = list[CmfTransactionCountRawObservation] | list[CmfPurchaseVolumeRawObservation]
FetchLatestSourceMonth = Callable[[httpx.AsyncClient, str, date], Awaitable[date | None]]
SyncDataset = Callable[[httpx.AsyncClient, object, str, date], Awaitable[int]]


@dataclass(frozen=True)
class CmfMonthlyDataset:
    dataset_code: str
    fetch_latest_source_month: FetchLatestSourceMonth
    sync_dataset: SyncDataset


@dataclass(frozen=True)
class CmfMonthlyWorkerConfig:
    supabase_url: str
    supabase_service_role_key: str
    endpoint_base: str = DEFAULT_CMF_ENDPOINT_BASE
    sync_interval_s: int = DAILY_INTERVAL_S


def load_config() -> CmfMonthlyWorkerConfig:
    load_dotenv()
    return CmfMonthlyWorkerConfig(
        supabase_url=os.environ["SUPABASE_URL"],
        supabase_service_role_key=os.environ["SUPABASE_SERVICE_ROLE_KEY"],
        endpoint_base=os.environ.get("BASE_ENDPOINT_CMF_CARDS", DEFAULT_CMF_ENDPOINT_BASE),
    )


def latest_observation_month(observations: RawObservations) -> date | None:
    if not observations:
        return None

    return max(observation.period_month for observation in observations)


async def fetch_transaction_count_latest_source_month(
    client: httpx.AsyncClient,
    endpoint_base: str,
    run_date: date,
) -> date | None:
    observations = await fetch_transaction_count_observations(
        client,
        endpoint_base=endpoint_base,
        fecha_fin=run_date,
    )
    return latest_observation_month(observations)


async def fetch_purchase_volume_latest_source_month(
    client: httpx.AsyncClient,
    endpoint_base: str,
    run_date: date,
) -> date | None:
    observations = await fetch_purchase_volume_observations(
        client,
        endpoint_base=endpoint_base,
        fecha_fin=run_date,
    )
    return latest_observation_month(observations)


async def sync_transaction_count_dataset(
    client: httpx.AsyncClient,
    sb,
    endpoint_base: str,
    run_date: date,
) -> int:
    return await sync_transaction_count_once(
        client,
        sb,
        config=TransactionCountConfig(endpoint_base=endpoint_base),
        run_date=run_date,
    )


async def sync_purchase_volume_dataset(
    client: httpx.AsyncClient,
    sb,
    endpoint_base: str,
    run_date: date,
) -> int:
    return await sync_purchase_volume_once(
        client,
        sb,
        config=PurchaseVolumeConfig(endpoint_base=endpoint_base),
        run_date=run_date,
    )


def active_monthly_datasets() -> list[CmfMonthlyDataset]:
    return [
        CmfMonthlyDataset(
            dataset_code=BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET,
            fetch_latest_source_month=fetch_transaction_count_latest_source_month,
            sync_dataset=sync_transaction_count_dataset,
        ),
        CmfMonthlyDataset(
            dataset_code=BANK_CREDIT_CARD_PURCHASE_VOLUME_DATASET,
            fetch_latest_source_month=fetch_purchase_volume_latest_source_month,
            sync_dataset=sync_purchase_volume_dataset,
        ),
    ]


async def sync_cmf_monthly_dataset_once(
    client: httpx.AsyncClient,
    sb,
    *,
    dataset: CmfMonthlyDataset,
    endpoint_base: str,
    run_date: date,
) -> int:
    record_cmf_sync_attempt(sb, dataset.dataset_code)
    latest_source_month = await dataset.fetch_latest_source_month(
        client,
        endpoint_base,
        run_date,
    )

    if latest_source_month is None:
        log.info("Skipping %s: source returned no rows.", dataset.dataset_code)
        return 0

    latest_state_month = get_latest_state_source_month(sb, dataset.dataset_code)
    if latest_state_month is not None and latest_source_month <= latest_state_month:
        log.info("Skipping %s: latest source month is unchanged.", dataset.dataset_code)
        return 0

    try:
        rows_synced = await dataset.sync_dataset(client, sb, endpoint_base, run_date)
    except Exception as exc:
        record_cmf_sync_failure(sb, dataset_code=dataset.dataset_code, error=exc)
        raise

    record_cmf_sync_success(
        sb,
        dataset_code=dataset.dataset_code,
        latest_source_month=latest_source_month,
        latest_curated_month=latest_source_month,
    )
    return rows_synced


async def sync_all_cmf_monthly_datasets_once(
    client: httpx.AsyncClient,
    sb,
    *,
    config: CmfMonthlyWorkerConfig,
    run_date: date,
    datasets: list[CmfMonthlyDataset] | None = None,
) -> dict[str, int]:
    results: dict[str, int] = {}

    for dataset in datasets or active_monthly_datasets():
        results[dataset.dataset_code] = await sync_cmf_monthly_dataset_once(
            client,
            sb,
            dataset=dataset,
            endpoint_base=config.endpoint_base,
            run_date=run_date,
        )

    return results


async def run_worker(config: CmfMonthlyWorkerConfig | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    worker_config = config or load_config()
    sb = create_client(
        worker_config.supabase_url,
        worker_config.supabase_service_role_key,
    )

    async with httpx.AsyncClient() as client:
        while True:
            try:
                await sync_all_cmf_monthly_datasets_once(
                    client,
                    sb,
                    config=worker_config,
                    run_date=date.today(),
                )
            except Exception as exc:
                log.warning("CMF monthly sync failed: %s: %s", type(exc).__name__, exc)

            await asyncio.sleep(worker_config.sync_interval_s)

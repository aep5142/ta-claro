import asyncio
import logging
import os
from dataclasses import dataclass

import httpx
from dotenv import load_dotenv
from supabase import create_client

from data.loaders.uf_loader import (
    latest_stored_uf_date,
    new_uf_values,
    record_uf_sync_failure,
    record_uf_sync_success,
    upsert_uf_values,
)
from data.sources.uf_source import fetch_historical_ufs

SYNC_INTERVAL_S = 5 * 24 * 60 * 60

log = logging.getLogger("uf-worker")


@dataclass(frozen=True)
class UfWorkerConfig:
    cmf_api_key: str
    base_endpoint_cmf_uf: str
    supabase_url: str
    supabase_service_role_key: str
    sync_interval_s: int = SYNC_INTERVAL_S


def load_config() -> UfWorkerConfig:
    load_dotenv()
    return UfWorkerConfig(
        cmf_api_key=os.environ["CMF_API_KEY"],
        base_endpoint_cmf_uf=os.environ["BASE_ENDPOINT_CMF_UF"],
        supabase_url=os.environ["SUPABASE_URL"],
        supabase_service_role_key=os.environ["SUPABASE_SERVICE_ROLE_KEY"],
    )


async def sync_uf_once(client, sb, config: UfWorkerConfig) -> int:
    try:
        source_values = await fetch_historical_ufs(
            client,
            template=config.base_endpoint_cmf_uf,
            api_key=config.cmf_api_key,
        )

        if not source_values:
            log.info("Skipping UF sync: source returned no UF rows.")
            return 0

        latest_source_date = source_values[-1].uf_date
        stored_date = latest_stored_uf_date(sb)

        if stored_date is not None and latest_source_date <= stored_date:
            log.info("Skipping UF sync: latest source UF date is unchanged.")
            record_uf_sync_success(
                sb,
                latest_source_date=latest_source_date,
                latest_stored_date=stored_date,
                rows_upserted=0,
            )
            return 0

        values_to_upsert = new_uf_values(source_values, stored_date)
        upsert_uf_values(sb, values_to_upsert)
        new_latest_stored_date = values_to_upsert[-1].uf_date
        record_uf_sync_success(
            sb,
            latest_source_date=latest_source_date,
            latest_stored_date=new_latest_stored_date,
            rows_upserted=len(values_to_upsert),
        )
        log.info("UF sync completed successfully with %s new rows.", len(values_to_upsert))
        return len(values_to_upsert)
    except Exception as exc:
        record_uf_sync_failure(sb, exc)
        raise


async def run_worker(config: UfWorkerConfig | None = None) -> None:
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
                await sync_uf_once(client, sb, worker_config)
            except Exception as exc:
                log.warning("UF sync failed: %s: %s", type(exc).__name__, exc)

            await asyncio.sleep(worker_config.sync_interval_s)

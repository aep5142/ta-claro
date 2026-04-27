import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import date

import httpx
from dotenv import load_dotenv
from supabase import create_client

from data.loaders.bank_credit_card_ops_loader import (
    get_uf_value_for_date,
    upsert_bank_credit_card_ops_curated,
    upsert_bank_credit_card_ops_raw,
)
from data.loaders.bank_credit_card_ops_sync_state_loader import (
    get_latest_state_source_month,
    record_sync_attempt,
    record_sync_failure,
    record_sync_success,
)
from data.models.bank_credit_card_operations import (
    BANK_CREDIT_CARD_OPS_REGISTRY_TABLE,
    BankCreditCardOperationConfig,
    BankCreditCardOpsRawObservation,
)
from data.sources.bank_credit_card_operations import fetch_operation_batch
from data.transforms.bank_credit_card_ops import to_curated_bank_credit_card_ops

DAILY_INTERVAL_S = 24 * 60 * 60
DEFAULT_CMF_ENDPOINT_BASE = "https://best-sbif-api.azurewebsites.net/Cuadrosv2"

log = logging.getLogger("bank-credit-card-ops-worker")

@dataclass(frozen=True)
class BankCreditCardOpsWorkerConfig:
    supabase_url: str
    supabase_service_role_key: str
    endpoint_base: str = DEFAULT_CMF_ENDPOINT_BASE
    sync_interval_s: int = DAILY_INTERVAL_S


def load_config() -> BankCreditCardOpsWorkerConfig:
    load_dotenv()
    return BankCreditCardOpsWorkerConfig(
        supabase_url=os.environ["SUPABASE_URL"],
        supabase_service_role_key=os.environ["SUPABASE_SERVICE_ROLE_KEY"],
        endpoint_base=os.environ.get("BASE_ENDPOINT_CMF_CARDS", DEFAULT_CMF_ENDPOINT_BASE),
    )


def load_active_operation_configs(sb) -> list[BankCreditCardOperationConfig]:
    response = (
        sb.table(BANK_CREDIT_CARD_OPS_REGISTRY_TABLE)
        .select(
            "operation_type,dataset_code,transaction_count_source_tag,nominal_volume_source_tag,"
            "source_nombre,source_description,source_endpoint_base,refresh_frequency,start_date,is_active"
        )
        .eq("is_active", True)
        .execute()
    )

    return [
        BankCreditCardOperationConfig.from_row(row)
        for row in (response.data or [])
    ]


def latest_observation_month(observations: list[BankCreditCardOpsRawObservation]) -> date | None:
    if not observations:
        return None

    return max(observation.period_month for observation in observations)


async def sync_operation_once(
    client: httpx.AsyncClient,
    sb,
    *,
    config: BankCreditCardOperationConfig,
    run_date: date,
) -> int:
    record_sync_attempt(sb, config.dataset_code)
    try:
        batch = await fetch_operation_batch(
            client,
            config=config,
            fecha_fin=run_date,
        )

        if batch.latest_source_month is None:
            log.info("Skipping %s: source returned no rows.", config.dataset_code)
            return 0

        latest_state_month = get_latest_state_source_month(sb, config.dataset_code)
        if latest_state_month is not None and batch.latest_source_month <= latest_state_month:
            log.info("Skipping %s: latest source month is unchanged.", config.dataset_code)
            return 0

        curated_observations = to_curated_bank_credit_card_ops(
            batch.raw_observations,
            uf_lookup=lambda uf_date: get_uf_value_for_date(sb, uf_date),
        )
        upsert_bank_credit_card_ops_raw(sb, batch.raw_observations)
        upsert_bank_credit_card_ops_curated(sb, curated_observations)
    except Exception as exc:
        record_sync_failure(sb, dataset_code=config.dataset_code, error=exc)
        raise

    record_sync_success(
        sb,
        dataset_code=config.dataset_code,
        latest_source_month=batch.latest_source_month,
        latest_curated_month=batch.latest_source_month,
    )
    return len(batch.raw_observations)


async def sync_all_bank_credit_card_ops_once(
    client: httpx.AsyncClient,
    sb,
    *,
    config: BankCreditCardOpsWorkerConfig,
    run_date: date,
    operations: list[BankCreditCardOperationConfig] | None = None,
) -> dict[str, int]:
    results: dict[str, int] = {}

    for operation in operations or load_active_operation_configs(sb):
        try:
            results[operation.dataset_code] = await sync_operation_once(
                client,
                sb,
                config=operation,
                run_date=run_date,
            )
        except Exception as exc:
            log.warning(
                "Bank credit-card operation %s failed: %s: %s",
                operation.dataset_code,
                type(exc).__name__,
                exc,
            )
            results[operation.dataset_code] = 0

    return results


async def run_worker(config: BankCreditCardOpsWorkerConfig | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    worker_config = config or load_config()
    sb = create_client(
        worker_config.supabase_url,
        worker_config.supabase_service_role_key,
    )

    async with httpx.AsyncClient() as client:
        while True:
            try:
                await sync_all_bank_credit_card_ops_once(
                    client,
                    sb,
                    config=worker_config,
                    run_date=date.today(),
                )
            except Exception as exc:
                log.warning("Bank credit-card ops sync failed: %s: %s", type(exc).__name__, exc)

            await asyncio.sleep(worker_config.sync_interval_s)

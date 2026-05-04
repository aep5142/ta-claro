import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import httpx
from dotenv import load_dotenv
from supabase import create_client

from data.loaders.bank_credit_card_ops_loader import (
    earliest_curated_card_count_month,
    earliest_curated_operation_month,
    get_uf_value_for_date,
    upsert_bank_credit_card_count_raw,
    upsert_bank_credit_card_counts_curated,
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
    BANK_CREDIT_CARD_ACTIVE_CARDS_PRIMARY_DATASET,
    BANK_CREDIT_CARD_ACTIVE_CARDS_NON_BANKING_DATASET,
    BANK_CREDIT_CARD_ACTIVE_CARDS_SUPPLEMENTARY_DATASET,
    BANK_CREDIT_CARD_OPERATION_AVANCE_EN_EFECTIVO,
    BANK_CREDIT_CARD_OPERATION_CARGOS_POR_SERVICIO,
    BANK_CREDIT_CARD_OPERATION_COMPRAS_NON_BANKING,
    BANK_CREDIT_CARD_OPERATION_COMPRAS,
    BANK_CREDIT_CARD_CARDS_WITH_OPERATIONS_PRIMARY_DATASET,
    BANK_CREDIT_CARD_CARDS_WITH_OPERATIONS_NON_BANKING_DATASET,
    BANK_CREDIT_CARD_CARDS_WITH_OPERATIONS_SUPPLEMENTARY_DATASET,
    BANK_CREDIT_CARD_COUNTS_DATASET,
    BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_DATASET,
    BANK_CREDIT_CARD_OPS_CARGOS_POR_SERVICIO_DATASET,
    BANK_CREDIT_CARD_OPS_COMPRAS_DATASET,
    BANK_CREDIT_CARD_OPS_NON_BANKING_COMPRAS_DATASET,
    CMF_MEASURE_KIND_ACTIVE_CARDS_NON_BANKING,
    CMF_MEASURE_KIND_ACTIVE_CARDS_PRIMARY,
    CMF_MEASURE_KIND_ACTIVE_CARDS_SUPPLEMENTARY,
    CMF_MEASURE_KIND_CARDS_WITH_OPERATIONS_NON_BANKING,
    CMF_MEASURE_KIND_CARDS_WITH_OPERATIONS_PRIMARY,
    CMF_MEASURE_KIND_CARDS_WITH_OPERATIONS_SUPPLEMENTARY,
    CMF_MEASURE_KIND_NOMINAL_VOLUME,
    CMF_MEASURE_KIND_TRANSACTION_COUNT,
    CMF_DATASETS_TABLE,
    BankCreditCardCountsConfig,
    BankCreditCardEndpointConfig,
    BankCreditCardOperationConfig,
    BankCreditCardOpsRawObservation,
)
from data.sources.bank_credit_card_operations import (
    fetch_card_counts_batch,
    fetch_operation_batch,
)
from data.transforms.bank_credit_card_ops import (
    to_curated_bank_credit_card_counts,
    to_curated_bank_credit_card_ops,
)

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


def operation_dataset_code(operation_type: str) -> str:
    if operation_type == BANK_CREDIT_CARD_OPERATION_COMPRAS:
        return BANK_CREDIT_CARD_OPS_COMPRAS_DATASET
    if operation_type == BANK_CREDIT_CARD_OPERATION_COMPRAS_NON_BANKING:
        return BANK_CREDIT_CARD_OPS_NON_BANKING_COMPRAS_DATASET
    if operation_type == BANK_CREDIT_CARD_OPERATION_AVANCE_EN_EFECTIVO:
        return BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_DATASET
    if operation_type == BANK_CREDIT_CARD_OPERATION_CARGOS_POR_SERVICIO:
        return BANK_CREDIT_CARD_OPS_CARGOS_POR_SERVICIO_DATASET

    raise ValueError(f"Unsupported operation type: {operation_type}")


def load_active_operation_configs(sb) -> list[BankCreditCardOperationConfig]:
    response = (
        sb.table(CMF_DATASETS_TABLE)
        .select(
            "operation_type,dataset_code,measure_kind,source_tag,source_nombre,"
            "source_description,source_endpoint_base,refresh_frequency,start_date,is_active"
        )
        .eq("is_active", True)
        .execute()
    )
    endpoints_by_operation: dict[str, dict[str, BankCreditCardEndpointConfig]] = {}
    for row in response.data or []:
        if not row.get("operation_type") or not row.get("measure_kind") or not row.get("source_tag"):
            continue

        endpoint = BankCreditCardEndpointConfig.from_row(row)
        endpoints_by_operation.setdefault(endpoint.operation_type, {})[
            endpoint.measure_kind
        ] = endpoint

    operations: list[BankCreditCardOperationConfig] = []
    for operation_type, endpoint_group in endpoints_by_operation.items():
        transaction_count_endpoint = endpoint_group.get(CMF_MEASURE_KIND_TRANSACTION_COUNT)
        nominal_volume_endpoint = endpoint_group.get(CMF_MEASURE_KIND_NOMINAL_VOLUME)
        if transaction_count_endpoint is None or nominal_volume_endpoint is None:
            continue

        operations.append(
            BankCreditCardOperationConfig(
                operation_type=operation_type,
                dataset_code=operation_dataset_code(operation_type),
                transaction_count_dataset_code=transaction_count_endpoint.dataset_code,
                nominal_volume_dataset_code=nominal_volume_endpoint.dataset_code,
                transaction_count_source_tag=transaction_count_endpoint.source_tag,
                nominal_volume_source_tag=nominal_volume_endpoint.source_tag,
                source_nombre=transaction_count_endpoint.source_nombre,
                source_description=transaction_count_endpoint.source_description,
                source_endpoint_base=transaction_count_endpoint.source_endpoint_base,
                refresh_frequency=transaction_count_endpoint.refresh_frequency,
                start_date=transaction_count_endpoint.start_date,
            )
        )

    return sorted(operations, key=lambda operation: operation.dataset_code)


def load_active_card_counts_config(sb) -> BankCreditCardCountsConfig | None:
    response = (
        sb.table(CMF_DATASETS_TABLE)
        .select(
            "dataset_code,measure_kind,source_tag,source_endpoint_base,refresh_frequency,start_date,is_active"
        )
        .eq("is_active", True)
        .execute()
    )

    endpoints_by_measure_kind: dict[str, BankCreditCardEndpointConfig] = {}
    for row in response.data or []:
        measure_kind = row.get("measure_kind")
        if measure_kind not in {
            CMF_MEASURE_KIND_ACTIVE_CARDS_PRIMARY,
            CMF_MEASURE_KIND_ACTIVE_CARDS_SUPPLEMENTARY,
            CMF_MEASURE_KIND_CARDS_WITH_OPERATIONS_PRIMARY,
            CMF_MEASURE_KIND_CARDS_WITH_OPERATIONS_SUPPLEMENTARY,
            CMF_MEASURE_KIND_ACTIVE_CARDS_NON_BANKING,
            CMF_MEASURE_KIND_CARDS_WITH_OPERATIONS_NON_BANKING,
        }:
            continue

        endpoints_by_measure_kind[measure_kind] = BankCreditCardEndpointConfig.from_row(
            {
                "operation_type": row.get("operation_type"),
                "dataset_code": row["dataset_code"],
                "measure_kind": row["measure_kind"],
                "source_tag": row["source_tag"],
                "source_nombre": row.get("source_nombre", row["dataset_code"]),
                "source_description": row.get("source_description", row["dataset_code"]),
                "source_endpoint_base": row["source_endpoint_base"],
                "refresh_frequency": row["refresh_frequency"],
                "start_date": row["start_date"],
                "is_active": row.get("is_active", True),
            }
        )

    required = {
        CMF_MEASURE_KIND_ACTIVE_CARDS_PRIMARY,
        CMF_MEASURE_KIND_ACTIVE_CARDS_SUPPLEMENTARY,
        CMF_MEASURE_KIND_CARDS_WITH_OPERATIONS_PRIMARY,
        CMF_MEASURE_KIND_CARDS_WITH_OPERATIONS_SUPPLEMENTARY,
    }
    if not required.issubset(endpoints_by_measure_kind):
        return None

    primary = endpoints_by_measure_kind[CMF_MEASURE_KIND_ACTIVE_CARDS_PRIMARY]
    supplementary = endpoints_by_measure_kind[
        CMF_MEASURE_KIND_ACTIVE_CARDS_SUPPLEMENTARY
    ]
    operating_primary = endpoints_by_measure_kind[
        CMF_MEASURE_KIND_CARDS_WITH_OPERATIONS_PRIMARY
    ]
    operating_supplementary = endpoints_by_measure_kind[
        CMF_MEASURE_KIND_CARDS_WITH_OPERATIONS_SUPPLEMENTARY
    ]
    non_banking_active = endpoints_by_measure_kind.get(
        CMF_MEASURE_KIND_ACTIVE_CARDS_NON_BANKING
    )
    non_banking_operating = endpoints_by_measure_kind.get(
        CMF_MEASURE_KIND_CARDS_WITH_OPERATIONS_NON_BANKING
    )
    return BankCreditCardCountsConfig(
        dataset_code=BANK_CREDIT_CARD_COUNTS_DATASET,
        active_cards_primary_dataset_code=primary.dataset_code,
        active_cards_supplementary_dataset_code=supplementary.dataset_code,
        cards_with_operations_primary_dataset_code=operating_primary.dataset_code,
        cards_with_operations_supplementary_dataset_code=operating_supplementary.dataset_code,
        active_cards_non_banking_dataset_code=(
            non_banking_active.dataset_code if non_banking_active is not None else None
        ),
        cards_with_operations_non_banking_dataset_code=(
            non_banking_operating.dataset_code
            if non_banking_operating is not None
            else None
        ),
        active_cards_primary_source_tag=primary.source_tag,
        active_cards_supplementary_source_tag=supplementary.source_tag,
        cards_with_operations_primary_source_tag=operating_primary.source_tag,
        cards_with_operations_supplementary_source_tag=operating_supplementary.source_tag,
        active_cards_non_banking_source_tag=(
            non_banking_active.source_tag if non_banking_active is not None else None
        ),
        cards_with_operations_non_banking_source_tag=(
            non_banking_operating.source_tag
            if non_banking_operating is not None
            else None
        ),
        source_endpoint_base=primary.source_endpoint_base,
        refresh_frequency=primary.refresh_frequency,
        start_date=min(
            primary.start_date,
            supplementary.start_date,
            operating_primary.start_date,
            operating_supplementary.start_date,
        ),
    )


def latest_observation_month(observations: list[BankCreditCardOpsRawObservation]) -> date | None:
    if not observations:
        return None

    return max(observation.period_month for observation in observations)


def build_active_cards_lookup(sb):
    page_size = 1000
    offset = 0
    totals: dict[tuple[str, date], Decimal] = {}

    while True:
        response = (
            sb.table("bank_credit_card_counts_curated")
            .select("institution_code,period_month,total_active_cards")
            .eq("dataset_code", BANK_CREDIT_CARD_COUNTS_DATASET)
            .order("institution_code", desc=False)
            .order("period_month", desc=False)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        rows = response.data or []
        for row in rows:
            if row.get("total_active_cards") is None:
                continue
            totals[(row["institution_code"], date.fromisoformat(row["period_month"]))] = Decimal(
                str(row["total_active_cards"])
            )

        if len(rows) < page_size:
            break
        offset += page_size

    def lookup(institution_code: str, period_month: date) -> Decimal | None:
        return totals.get((institution_code, period_month))

    return lookup


async def sync_operation_once(
    client: httpx.AsyncClient,
    sb,
    *,
    config: BankCreditCardOperationConfig,
    run_date: date,
) -> int:
    record_sync_attempt(sb, config.transaction_count_dataset_code)
    record_sync_attempt(sb, config.nominal_volume_dataset_code)
    try:
        batch = await fetch_operation_batch(
            client,
            config=config,
            fecha_fin=run_date,
        )

        if batch.latest_source_month is None:
            log.info("Skipping %s: source returned no rows.", config.dataset_code)
            return 0

        latest_transaction_count_state_month = get_latest_state_source_month(
            sb,
            config.transaction_count_dataset_code,
        )
        latest_nominal_volume_state_month = get_latest_state_source_month(
            sb,
            config.nominal_volume_dataset_code,
        )
        earliest_curated_month = earliest_curated_operation_month(
            sb,
            dataset_code=config.dataset_code,
        )
        transaction_count_unchanged = (
            batch.latest_transaction_count_source_month is None
            or (
                latest_transaction_count_state_month is not None
                and batch.latest_transaction_count_source_month
                <= latest_transaction_count_state_month
            )
        )
        nominal_volume_unchanged = (
            batch.latest_nominal_volume_source_month is None
            or (
                latest_nominal_volume_state_month is not None
                and batch.latest_nominal_volume_source_month <= latest_nominal_volume_state_month
            )
        )
        history_is_complete = (
            batch.earliest_source_month is None
            or (
                earliest_curated_month is not None
                and earliest_curated_month <= batch.earliest_source_month
            )
        )
        if transaction_count_unchanged and nominal_volume_unchanged and history_is_complete:
            log.info("Skipping %s: latest source month is unchanged.", config.dataset_code)
            return 0

        curated_observations = to_curated_bank_credit_card_ops(
            batch.raw_observations,
            uf_lookup=lambda uf_date: get_uf_value_for_date(sb, uf_date),
            active_cards_lookup=build_active_cards_lookup(sb),
        )
        upsert_bank_credit_card_ops_raw(sb, batch.raw_observations)
        upsert_bank_credit_card_ops_curated(sb, curated_observations)
    except Exception as exc:
        record_sync_failure(sb, dataset_code=config.transaction_count_dataset_code, error=exc)
        record_sync_failure(sb, dataset_code=config.nominal_volume_dataset_code, error=exc)
        raise

    record_sync_success(
        sb,
        dataset_code=config.transaction_count_dataset_code,
        latest_source_month=batch.latest_transaction_count_source_month
        or batch.latest_source_month,
        latest_curated_month=batch.latest_source_month,
    )
    record_sync_success(
        sb,
        dataset_code=config.nominal_volume_dataset_code,
        latest_source_month=batch.latest_nominal_volume_source_month or batch.latest_source_month,
        latest_curated_month=batch.latest_source_month,
    )
    return len(batch.raw_observations)


async def sync_card_counts_once(
    client: httpx.AsyncClient,
    sb,
    *,
    config: BankCreditCardCountsConfig,
    run_date: date,
) -> int:
    dataset_codes = [
        config.active_cards_primary_dataset_code,
        config.active_cards_supplementary_dataset_code,
        config.cards_with_operations_primary_dataset_code,
        config.cards_with_operations_supplementary_dataset_code,
    ]
    if config.active_cards_non_banking_dataset_code:
        dataset_codes.append(config.active_cards_non_banking_dataset_code)
    if config.cards_with_operations_non_banking_dataset_code:
        dataset_codes.append(config.cards_with_operations_non_banking_dataset_code)
    for dataset_code in dataset_codes:
        record_sync_attempt(sb, dataset_code)

    try:
        batch = await fetch_card_counts_batch(client, config=config, fecha_fin=run_date)
        if batch.latest_source_month is None:
            log.info("Skipping %s: source returned no rows.", config.dataset_code)
            return 0

        latest_state_months = {
            dataset_code: get_latest_state_source_month(sb, dataset_code)
            for dataset_code in dataset_codes
        }
        latest_batch_months = {
            config.active_cards_primary_dataset_code: batch.latest_active_cards_primary_source_month,
            config.active_cards_supplementary_dataset_code: batch.latest_active_cards_supplementary_source_month,
            config.cards_with_operations_primary_dataset_code: batch.latest_cards_with_operations_primary_source_month,
            config.cards_with_operations_supplementary_dataset_code: batch.latest_cards_with_operations_supplementary_source_month,
        }
        if config.active_cards_non_banking_dataset_code:
            latest_batch_months[config.active_cards_non_banking_dataset_code] = (
                batch.latest_active_cards_non_banking_source_month
            )
        if config.cards_with_operations_non_banking_dataset_code:
            latest_batch_months[config.cards_with_operations_non_banking_dataset_code] = (
                batch.latest_cards_with_operations_non_banking_source_month
            )
        all_unchanged = True
        for dataset_code, latest_source_month in latest_batch_months.items():
            current_state_month = latest_state_months[dataset_code]
            if latest_source_month is None:
                continue
            if current_state_month is None or latest_source_month > current_state_month:
                all_unchanged = False
                break

        earliest_curated_month = earliest_curated_card_count_month(
            sb,
            dataset_code=config.dataset_code,
        )
        history_is_complete = (
            batch.earliest_source_month is None
            or (
                earliest_curated_month is not None
                and earliest_curated_month <= batch.earliest_source_month
            )
        )
        if all_unchanged and history_is_complete:
            log.info("Skipping %s: latest source month is unchanged.", config.dataset_code)
            return 0

        curated_observations = to_curated_bank_credit_card_counts(batch.raw_observations)
        upsert_bank_credit_card_count_raw(sb, batch.raw_observations)
        upsert_bank_credit_card_counts_curated(sb, curated_observations)
    except Exception as exc:
        for dataset_code in dataset_codes:
            record_sync_failure(sb, dataset_code=dataset_code, error=exc)
        raise

    latest_source_months = {
        config.active_cards_primary_dataset_code: batch.latest_active_cards_primary_source_month,
        config.active_cards_supplementary_dataset_code: batch.latest_active_cards_supplementary_source_month,
        config.cards_with_operations_primary_dataset_code: batch.latest_cards_with_operations_primary_source_month,
        config.cards_with_operations_supplementary_dataset_code: batch.latest_cards_with_operations_supplementary_source_month,
    }
    if config.active_cards_non_banking_dataset_code:
        latest_source_months[config.active_cards_non_banking_dataset_code] = (
            batch.latest_active_cards_non_banking_source_month
        )
    if config.cards_with_operations_non_banking_dataset_code:
        latest_source_months[config.cards_with_operations_non_banking_dataset_code] = (
            batch.latest_cards_with_operations_non_banking_source_month
        )
    for dataset_code, latest_source_month in latest_source_months.items():
        if latest_source_month is None:
            continue
        record_sync_success(
            sb,
            dataset_code=dataset_code,
            latest_source_month=latest_source_month,
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
    card_counts: BankCreditCardCountsConfig | None = None,
) -> dict[str, int]:
    results: dict[str, int] = {}

    card_counts_config = card_counts or load_active_card_counts_config(sb)
    if card_counts_config is not None:
        try:
            results[card_counts_config.dataset_code] = await sync_card_counts_once(
                client,
                sb,
                config=card_counts_config,
                run_date=run_date,
            )
        except Exception as exc:
            log.warning(
                "Bank credit-card counts %s failed: %s: %s",
                card_counts_config.dataset_code,
                type(exc).__name__,
                exc,
            )
            results[card_counts_config.dataset_code] = 0

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

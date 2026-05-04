from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlencode

from data.models.bank_credit_card_operations import (
    BANK_CREDIT_CARD_ACTIVE_CARDS_PRIMARY_DATASET,
    BANK_CREDIT_CARD_ACTIVE_CARDS_SUPPLEMENTARY_DATASET,
    BANK_CREDIT_CARD_CARDS_WITH_OPERATIONS_PRIMARY_DATASET,
    BANK_CREDIT_CARD_CARDS_WITH_OPERATIONS_SUPPLEMENTARY_DATASET,
    BankCreditCardOperationConfig,
    BankCreditCardCountObservation,
    BankCreditCardCountRawObservation,
    BankCreditCardCountsConfig,
    BankCreditCardOpsMeasureObservation,
    BankCreditCardOpsRawObservation,
)


@dataclass(frozen=True)
class BankCreditCardOpsObservationBatch:
    raw_observations: list[BankCreditCardOpsRawObservation]
    latest_source_month: date | None
    earliest_source_month: date | None
    latest_transaction_count_source_month: date | None
    latest_nominal_volume_source_month: date | None


@dataclass(frozen=True)
class BankCreditCardCountsObservationBatch:
    raw_observations: list[BankCreditCardCountRawObservation]
    latest_source_month: date | None
    earliest_source_month: date | None
    latest_active_cards_primary_source_month: date | None
    latest_active_cards_supplementary_source_month: date | None
    latest_cards_with_operations_primary_source_month: date | None
    latest_cards_with_operations_supplementary_source_month: date | None
    latest_active_cards_non_banking_source_month: date | None
    latest_cards_with_operations_non_banking_source_month: date | None


def build_cmf_cuadros_url(
    *,
    endpoint_base: str,
    tag: str,
    fecha_fin: date,
    fecha_inicio: str,
) -> str:
    query = urlencode(
        {
            "FechaFin": fecha_fin.strftime("%Y%m%d"),
            "FechaInicio": fecha_inicio,
            "Tag": tag,
            "from": "reload",
        }
    )
    return f"{endpoint_base}?{query}"


def derive_institution_code(source_codigo: str) -> str:
    parts = source_codigo.split("_")
    try:
        agifi_index = parts.index("AGIFI")
        institution_code = parts[agifi_index + 1]
    except (ValueError, IndexError) as exc:
        raise ValueError(f"Cannot derive institution_code from {source_codigo}") from exc

    # Non-banking card tags use AGIFI_MRC and then append issuer/brand tokens.
    # Using plain MRC collapses many institutions into one key, so we derive a
    # stable per-series code from the trailing identifier tokens.
    if institution_code == "MRC":
        trailing_parts = [
            token
            for token in parts[agifi_index + 2 :]
            if token and token not in {"$", "NUM"}
        ]
        if trailing_parts:
            institution_code = "_".join(trailing_parts)

    if not institution_code:
        raise ValueError(f"Cannot derive institution_code from {source_codigo}")

    return institution_code


def parse_cmf_numeric(raw_value: str | int | float | Decimal) -> Decimal:
    if isinstance(raw_value, Decimal):
        return raw_value

    if isinstance(raw_value, int):
        return Decimal(raw_value)

    if isinstance(raw_value, float):
        return Decimal(str(raw_value))

    normalized = raw_value.strip().replace(".", "").replace(",", ".")
    try:
        return Decimal(normalized)
    except InvalidOperation as exc:
        raise ValueError(f"Unsupported CMF numeric value: {raw_value}") from exc


def normalize_period_month(raw_period: str | int | date) -> date:
    if isinstance(raw_period, date):
        return raw_period.replace(day=1)

    if isinstance(raw_period, int):
        raw_period = str(raw_period)

    for date_format in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(raw_period, date_format).date().replace(day=1)
        except ValueError:
            continue

    for date_format in ("%Y%m", "%Y-%m"):
        try:
            return datetime.strptime(raw_period, date_format).date().replace(day=1)
        except ValueError:
            continue

    raise ValueError(f"Unsupported CMF period format: {raw_period}")


def parse_transaction_count_payload(
    payload: dict[str, Any],
    *,
    operation_type: str,
    dataset_code: str,
) -> list[BankCreditCardOpsMeasureObservation]:
    observations: list[BankCreditCardOpsMeasureObservation] = []

    for series in _get_series(payload):
        source_codigo = _first_present(series, "Codigo", "codigo", "source_codigo")
        institution_code = derive_institution_code(source_codigo)
        source_nombre = _first_present(
            series,
            "descripcionCorta",
            "DescripcionCorta",
            "Nombre",
            "nombre",
            "source_nombre",
        )
        source_series_id = str(_first_present(series, "id", "Id", "source_series_id"))

        for point in _get_observations(series):
            observations.append(
                BankCreditCardOpsMeasureObservation(
                    operation_type=operation_type,
                    dataset_code=dataset_code,
                    source_series_id=source_series_id,
                    source_codigo=source_codigo,
                    source_nombre=source_nombre,
                    institution_code=institution_code,
                    institution_name=source_nombre,
                    period_month=normalize_period_month(
                        _first_present(point, "Fecha", "fecha", "period", "Periodo")
                    ),
                    value=parse_cmf_numeric(_first_present(point, "Valor", "valor", "value")),
                    source_payload=point,
                )
            )

    return sorted(
        observations,
        key=lambda observation: (observation.institution_code, observation.period_month),
    )


def parse_card_count_payload(
    payload: dict[str, Any],
    *,
    dataset_code: str,
) -> list[BankCreditCardCountObservation]:
    observations: list[BankCreditCardCountObservation] = []

    for series in _get_series(payload):
        source_codigo = _first_present(series, "Codigo", "codigo", "source_codigo")
        institution_code = derive_institution_code(source_codigo)
        source_nombre = _first_present(
            series,
            "descripcionCorta",
            "DescripcionCorta",
            "Nombre",
            "nombre",
            "source_nombre",
        )
        source_series_id = str(_first_present(series, "id", "Id", "source_series_id"))

        for point in _get_observations(series):
            observations.append(
                BankCreditCardCountObservation(
                    dataset_code=dataset_code,
                    source_series_id=source_series_id,
                    source_codigo=source_codigo,
                    source_nombre=source_nombre,
                    institution_code=institution_code,
                    institution_name=source_nombre,
                    period_month=normalize_period_month(
                        _first_present(point, "Fecha", "fecha", "period", "Periodo")
                    ),
                    value=parse_cmf_numeric(_first_present(point, "Valor", "valor", "value")),
                    source_payload=point,
                )
            )

    return sorted(
        observations,
        key=lambda observation: (observation.institution_code, observation.period_month),
    )


def parse_nominal_volume_payload(
    payload: dict[str, Any],
    *,
    operation_type: str,
    dataset_code: str,
) -> list[BankCreditCardOpsMeasureObservation]:
    observations: list[BankCreditCardOpsMeasureObservation] = []

    for series in _get_series(payload):
        source_codigo = _first_present(series, "Codigo", "codigo", "source_codigo")
        institution_code = derive_institution_code(source_codigo)
        source_nombre = _first_present(
            series,
            "descripcionCorta",
            "DescripcionCorta",
            "Nombre",
            "nombre",
            "source_nombre",
        )
        source_series_id = str(_first_present(series, "id", "Id", "source_series_id"))

        for point in _get_observations(series):
            observations.append(
                BankCreditCardOpsMeasureObservation(
                    operation_type=operation_type,
                    dataset_code=dataset_code,
                    source_series_id=source_series_id,
                    source_codigo=source_codigo,
                    source_nombre=source_nombre,
                    institution_code=institution_code,
                    institution_name=source_nombre,
                    period_month=normalize_period_month(
                        _first_present(point, "Fecha", "fecha", "period", "Periodo")
                    ),
                    value=parse_cmf_numeric(_first_present(point, "Valor", "valor", "value")),
                    source_payload=point,
                )
            )

    return sorted(
        observations,
        key=lambda observation: (observation.institution_code, observation.period_month),
    )


def merge_operation_measure_observations(
    *,
    operation_type: str,
    dataset_code: str,
    transaction_count_observations: list[BankCreditCardOpsMeasureObservation],
    nominal_volume_observations: list[BankCreditCardOpsMeasureObservation],
) -> list[BankCreditCardOpsRawObservation]:
    count_by_key = {
        (observation.institution_code, observation.period_month): observation
        for observation in transaction_count_observations
    }
    volume_by_key = {
        (observation.institution_code, observation.period_month): observation
        for observation in nominal_volume_observations
    }

    keys = sorted(set(count_by_key) & set(volume_by_key))
    if not keys:
        return []

    raw_observations: list[BankCreditCardOpsRawObservation] = []
    for key in keys:
        count_observation = count_by_key[key]
        volume_observation = volume_by_key[key]
        raw_observations.append(
            BankCreditCardOpsRawObservation(
                operation_type=operation_type,
                dataset_code=dataset_code,
                source_series_id=volume_observation.source_series_id,
                source_codigo=volume_observation.source_codigo,
                source_nombre=volume_observation.source_nombre,
                institution_code=volume_observation.institution_code,
                institution_name=volume_observation.institution_name,
                period_month=volume_observation.period_month,
                transaction_count=count_observation.value,
                nominal_volume_millions_clp=volume_observation.value,
                source_payload={
                    "transaction_count": count_observation.source_payload,
                    "nominal_volume_millions_clp": volume_observation.source_payload,
                },
            )
        )

    return raw_observations


async def fetch_transaction_count_observations(
    client,
    *,
    endpoint_base: str,
    tag: str,
    fecha_inicio: str,
    fecha_fin: date,
    operation_type: str,
    dataset_code: str,
) -> list[BankCreditCardOpsMeasureObservation]:
    response = await client.get(
        build_cmf_cuadros_url(
            endpoint_base=endpoint_base,
            tag=tag,
            fecha_fin=fecha_fin,
            fecha_inicio=fecha_inicio,
        ),
        timeout=30,
    )
    response.raise_for_status()
    return parse_transaction_count_payload(
        response.json(),
        operation_type=operation_type,
        dataset_code=dataset_code,
    )


async def fetch_nominal_volume_observations(
    client,
    *,
    endpoint_base: str,
    tag: str,
    fecha_inicio: str,
    fecha_fin: date,
    operation_type: str,
    dataset_code: str,
) -> list[BankCreditCardOpsMeasureObservation]:
    response = await client.get(
        build_cmf_cuadros_url(
            endpoint_base=endpoint_base,
            tag=tag,
            fecha_fin=fecha_fin,
            fecha_inicio=fecha_inicio,
        ),
        timeout=30,
    )
    response.raise_for_status()
    return parse_nominal_volume_payload(
        response.json(),
        operation_type=operation_type,
        dataset_code=dataset_code,
    )


async def fetch_card_count_observations(
    client,
    *,
    endpoint_base: str,
    tag: str,
    fecha_inicio: str,
    fecha_fin: date,
    dataset_code: str,
) -> list[BankCreditCardCountObservation]:
    response = await client.get(
        build_cmf_cuadros_url(
            endpoint_base=endpoint_base,
            tag=tag,
            fecha_fin=fecha_fin,
            fecha_inicio=fecha_inicio,
        ),
        timeout=30,
    )
    response.raise_for_status()
    return parse_card_count_payload(response.json(), dataset_code=dataset_code)


async def fetch_operation_batch(
    client,
    *,
    config: BankCreditCardOperationConfig,
    fecha_fin: date,
) -> BankCreditCardOpsObservationBatch:
    transaction_count_observations = await fetch_transaction_count_observations(
        client,
        endpoint_base=config.source_endpoint_base,
        tag=config.transaction_count_source_tag,
        fecha_inicio=config.start_date.strftime("%Y%m%d"),
        fecha_fin=fecha_fin,
        operation_type=config.operation_type,
        dataset_code=config.dataset_code,
    )
    nominal_volume_observations = await fetch_nominal_volume_observations(
        client,
        endpoint_base=config.source_endpoint_base,
        tag=config.nominal_volume_source_tag,
        fecha_inicio=config.start_date.strftime("%Y%m%d"),
        fecha_fin=fecha_fin,
        operation_type=config.operation_type,
        dataset_code=config.dataset_code,
    )

    raw_observations = merge_operation_measure_observations(
        operation_type=config.operation_type,
        dataset_code=config.dataset_code,
        transaction_count_observations=transaction_count_observations,
        nominal_volume_observations=nominal_volume_observations,
    )
    latest_transaction_count_source_month = max(
        (observation.period_month for observation in transaction_count_observations),
        default=None,
    )
    latest_nominal_volume_source_month = max(
        (observation.period_month for observation in nominal_volume_observations),
        default=None,
    )
    latest_source_month = max(
        (
            latest_transaction_count_source_month,
            latest_nominal_volume_source_month,
        ),
        default=None,
    )
    return BankCreditCardOpsObservationBatch(
        raw_observations=raw_observations,
        latest_source_month=latest_source_month,
        earliest_source_month=min(
            (observation.period_month for observation in raw_observations),
            default=None,
        ),
        latest_transaction_count_source_month=latest_transaction_count_source_month,
        latest_nominal_volume_source_month=latest_nominal_volume_source_month,
    )


def to_card_count_raw_observations(
    observations: list[BankCreditCardCountObservation],
) -> list[BankCreditCardCountRawObservation]:
    return [
        BankCreditCardCountRawObservation(
            dataset_code=observation.dataset_code,
            source_series_id=observation.source_series_id,
            source_codigo=observation.source_codigo,
            source_nombre=observation.source_nombre,
            institution_code=observation.institution_code,
            institution_name=observation.institution_name,
            period_month=observation.period_month,
            card_count=observation.value,
            source_payload=observation.source_payload,
        )
        for observation in observations
    ]


async def fetch_card_counts_batch(
    client,
    *,
    config: BankCreditCardCountsConfig,
    fecha_fin: date,
) -> BankCreditCardCountsObservationBatch:
    active_primary = await fetch_card_count_observations(
        client,
        endpoint_base=config.source_endpoint_base,
        tag=config.active_cards_primary_source_tag,
        fecha_inicio=config.start_date.strftime("%Y%m%d"),
        fecha_fin=fecha_fin,
        dataset_code=config.active_cards_primary_dataset_code,
    )
    active_supplementary = await fetch_card_count_observations(
        client,
        endpoint_base=config.source_endpoint_base,
        tag=config.active_cards_supplementary_source_tag,
        fecha_inicio=config.start_date.strftime("%Y%m%d"),
        fecha_fin=fecha_fin,
        dataset_code=config.active_cards_supplementary_dataset_code,
    )
    operating_primary = await fetch_card_count_observations(
        client,
        endpoint_base=config.source_endpoint_base,
        tag=config.cards_with_operations_primary_source_tag,
        fecha_inicio=config.start_date.strftime("%Y%m%d"),
        fecha_fin=fecha_fin,
        dataset_code=config.cards_with_operations_primary_dataset_code,
    )
    operating_supplementary = await fetch_card_count_observations(
        client,
        endpoint_base=config.source_endpoint_base,
        tag=config.cards_with_operations_supplementary_source_tag,
        fecha_inicio=config.start_date.strftime("%Y%m%d"),
        fecha_fin=fecha_fin,
        dataset_code=config.cards_with_operations_supplementary_dataset_code,
    )
    non_banking_active: list[BankCreditCardCountObservation] = []
    if (
        config.active_cards_non_banking_source_tag
        and config.active_cards_non_banking_dataset_code
    ):
        non_banking_active = await fetch_card_count_observations(
            client,
            endpoint_base=config.source_endpoint_base,
            tag=config.active_cards_non_banking_source_tag,
            fecha_inicio=config.start_date.strftime("%Y%m%d"),
            fecha_fin=fecha_fin,
            dataset_code=config.active_cards_non_banking_dataset_code,
        )
    non_banking_operating: list[BankCreditCardCountObservation] = []
    if (
        config.cards_with_operations_non_banking_source_tag
        and config.cards_with_operations_non_banking_dataset_code
    ):
        non_banking_operating = await fetch_card_count_observations(
            client,
            endpoint_base=config.source_endpoint_base,
            tag=config.cards_with_operations_non_banking_source_tag,
            fecha_inicio=config.start_date.strftime("%Y%m%d"),
            fecha_fin=fecha_fin,
            dataset_code=config.cards_with_operations_non_banking_dataset_code,
        )
    raw_observations = [
        *to_card_count_raw_observations(active_primary),
        *to_card_count_raw_observations(active_supplementary),
        *to_card_count_raw_observations(operating_primary),
        *to_card_count_raw_observations(operating_supplementary),
        *to_card_count_raw_observations(non_banking_active),
        *to_card_count_raw_observations(non_banking_operating),
    ]

    return BankCreditCardCountsObservationBatch(
        raw_observations=sorted(
            raw_observations,
            key=lambda observation: (
                observation.dataset_code,
                observation.institution_code,
                observation.period_month,
            ),
        ),
        latest_source_month=max(
            (
                *(observation.period_month for observation in active_primary),
                *(observation.period_month for observation in active_supplementary),
                *(observation.period_month for observation in operating_primary),
                *(observation.period_month for observation in operating_supplementary),
                *(observation.period_month for observation in non_banking_active),
                *(observation.period_month for observation in non_banking_operating),
            ),
            default=None,
        ),
        earliest_source_month=min(
            (
                *(observation.period_month for observation in active_primary),
                *(observation.period_month for observation in active_supplementary),
                *(observation.period_month for observation in operating_primary),
                *(observation.period_month for observation in operating_supplementary),
                *(observation.period_month for observation in non_banking_active),
                *(observation.period_month for observation in non_banking_operating),
            ),
            default=None,
        ),
        latest_active_cards_primary_source_month=max(
            (observation.period_month for observation in active_primary),
            default=None,
        ),
        latest_active_cards_supplementary_source_month=max(
            (observation.period_month for observation in active_supplementary),
            default=None,
        ),
        latest_cards_with_operations_primary_source_month=max(
            (observation.period_month for observation in operating_primary),
            default=None,
        ),
        latest_cards_with_operations_supplementary_source_month=max(
            (observation.period_month for observation in operating_supplementary),
            default=None,
        ),
        latest_active_cards_non_banking_source_month=max(
            (observation.period_month for observation in non_banking_active),
            default=None,
        ),
        latest_cards_with_operations_non_banking_source_month=max(
            (observation.period_month for observation in non_banking_operating),
            default=None,
        ),
    )


def _get_series(payload: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("series", "Series"):
        if key in payload and isinstance(payload[key], list):
            return payload[key]

    raise ValueError("CMF payload does not include a series list")


def _get_observations(series: dict[str, Any]) -> list[dict[str, Any]]:
    for key in (
        "observaciones",
        "Observaciones",
        "data",
        "datos",
        "Datos",
        "observations",
        "valores",
        "Valores",
    ):
        if key in series and isinstance(series[key], list):
            return series[key]

    raise ValueError("CMF series does not include observations")


def _first_present(source: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = source.get(key)
        if value is not None:
            return value

    raise ValueError(f"Missing required CMF field. Tried: {', '.join(keys)}")

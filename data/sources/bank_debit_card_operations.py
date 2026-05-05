from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlencode

from data.models.bank_debit_card_operations import (
    BankDebitCardCountObservation,
    BankDebitCardCountRawObservation,
    BankDebitCardCountsConfig,
    BankDebitCardOperationConfig,
    BankDebitCardOpsMeasureObservation,
    BankDebitCardOpsRawObservation,
)


@dataclass(frozen=True)
class BankDebitCardOpsObservationBatch:
    raw_observations: list[BankDebitCardOpsRawObservation]
    latest_source_month: date | None
    earliest_source_month: date | None
    latest_transaction_count_source_month: date | None
    latest_nominal_volume_source_month: date | None


@dataclass(frozen=True)
class BankDebitCardCountsObservationBatch:
    raw_observations: list[BankDebitCardCountRawObservation]
    latest_source_month: date | None
    earliest_source_month: date | None
    latest_active_cards_primary_debit_source_month: date | None
    latest_active_cards_primary_atm_only_source_month: date | None
    latest_active_cards_supplementary_debit_source_month: date | None
    latest_active_cards_supplementary_atm_only_source_month: date | None
    latest_active_cards_total_debit_source_month: date | None
    latest_active_cards_total_atm_only_source_month: date | None
    latest_cards_with_operations_debit_source_month: date | None
    latest_cards_with_operations_atm_only_source_month: date | None


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
    institution_code = ""
    if "AGIFI" in parts:
        try:
            agifi_index = parts.index("AGIFI")
            institution_code = parts[agifi_index + 1]
        except IndexError as exc:
            raise ValueError(f"Cannot derive institution_code from {source_codigo}") from exc
    else:
        # Some CMF debit series omit AGIFI and encode bank code as *_<BANK>_NUM_MONT.
        if len(parts) >= 3 and parts[-2] in {"NUM", "MM$", "$"}:
            institution_code = parts[-3]

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
) -> list[BankDebitCardOpsMeasureObservation]:
    observations: list[BankDebitCardOpsMeasureObservation] = []

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
                BankDebitCardOpsMeasureObservation(
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

    return sorted(observations, key=lambda observation: (observation.institution_code, observation.period_month))


def parse_nominal_volume_payload(
    payload: dict[str, Any],
    *,
    operation_type: str,
    dataset_code: str,
) -> list[BankDebitCardOpsMeasureObservation]:
    observations: list[BankDebitCardOpsMeasureObservation] = []

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
                BankDebitCardOpsMeasureObservation(
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

    return sorted(observations, key=lambda observation: (observation.institution_code, observation.period_month))


def parse_card_count_payload(
    payload: dict[str, Any],
    *,
    dataset_code: str,
) -> list[BankDebitCardCountObservation]:
    observations: list[BankDebitCardCountObservation] = []

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
                BankDebitCardCountObservation(
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

    return sorted(observations, key=lambda observation: (observation.institution_code, observation.period_month))


def merge_operation_measure_observations(
    *,
    operation_type: str,
    dataset_code: str,
    transaction_count_observations: list[BankDebitCardOpsMeasureObservation],
    nominal_volume_observations: list[BankDebitCardOpsMeasureObservation],
) -> list[BankDebitCardOpsRawObservation]:
    count_by_key = {
        (observation.institution_code, observation.period_month): observation
        for observation in transaction_count_observations
    }
    volume_by_key = {
        (observation.institution_code, observation.period_month): observation
        for observation in nominal_volume_observations
    }

    raw_observations: list[BankDebitCardOpsRawObservation] = []
    for key in sorted(set(count_by_key) & set(volume_by_key)):
        count_observation = count_by_key[key]
        volume_observation = volume_by_key[key]
        raw_observations.append(
            BankDebitCardOpsRawObservation(
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
) -> list[BankDebitCardOpsMeasureObservation]:
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
    return parse_transaction_count_payload(response.json(), operation_type=operation_type, dataset_code=dataset_code)


async def fetch_nominal_volume_observations(
    client,
    *,
    endpoint_base: str,
    tag: str,
    fecha_inicio: str,
    fecha_fin: date,
    operation_type: str,
    dataset_code: str,
) -> list[BankDebitCardOpsMeasureObservation]:
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
    return parse_nominal_volume_payload(response.json(), operation_type=operation_type, dataset_code=dataset_code)


async def fetch_card_count_observations(
    client,
    *,
    endpoint_base: str,
    tag: str,
    fecha_inicio: str,
    fecha_fin: date,
    dataset_code: str,
) -> list[BankDebitCardCountObservation]:
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
    config: BankDebitCardOperationConfig,
    fecha_fin: date,
) -> BankDebitCardOpsObservationBatch:
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
        (latest_transaction_count_source_month, latest_nominal_volume_source_month),
        default=None,
    )
    return BankDebitCardOpsObservationBatch(
        raw_observations=raw_observations,
        latest_source_month=latest_source_month,
        earliest_source_month=min((observation.period_month for observation in raw_observations), default=None),
        latest_transaction_count_source_month=latest_transaction_count_source_month,
        latest_nominal_volume_source_month=latest_nominal_volume_source_month,
    )


def to_card_count_raw_observations(
    observations: list[BankDebitCardCountObservation],
) -> list[BankDebitCardCountRawObservation]:
    return [
        BankDebitCardCountRawObservation(
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
    config: BankDebitCardCountsConfig,
    fecha_fin: date,
) -> BankDebitCardCountsObservationBatch:
    active_primary_debit = await fetch_card_count_observations(
        client,
        endpoint_base=config.source_endpoint_base,
        tag=config.active_cards_primary_debit_source_tag,
        fecha_inicio=config.start_date.strftime("%Y%m%d"),
        fecha_fin=fecha_fin,
        dataset_code=config.active_cards_primary_debit_dataset_code,
    )
    active_primary_atm_only = await fetch_card_count_observations(
        client,
        endpoint_base=config.source_endpoint_base,
        tag=config.active_cards_primary_atm_only_source_tag,
        fecha_inicio=config.start_date.strftime("%Y%m%d"),
        fecha_fin=fecha_fin,
        dataset_code=config.active_cards_primary_atm_only_dataset_code,
    )
    active_supplementary_debit = await fetch_card_count_observations(
        client,
        endpoint_base=config.source_endpoint_base,
        tag=config.active_cards_supplementary_debit_source_tag,
        fecha_inicio=config.start_date.strftime("%Y%m%d"),
        fecha_fin=fecha_fin,
        dataset_code=config.active_cards_supplementary_debit_dataset_code,
    )
    active_supplementary_atm_only = await fetch_card_count_observations(
        client,
        endpoint_base=config.source_endpoint_base,
        tag=config.active_cards_supplementary_atm_only_source_tag,
        fecha_inicio=config.start_date.strftime("%Y%m%d"),
        fecha_fin=fecha_fin,
        dataset_code=config.active_cards_supplementary_atm_only_dataset_code,
    )
    active_total_debit = await fetch_card_count_observations(
        client,
        endpoint_base=config.source_endpoint_base,
        tag=config.active_cards_total_debit_source_tag,
        fecha_inicio=config.start_date.strftime("%Y%m%d"),
        fecha_fin=fecha_fin,
        dataset_code=config.active_cards_total_debit_dataset_code,
    )
    active_total_atm_only = await fetch_card_count_observations(
        client,
        endpoint_base=config.source_endpoint_base,
        tag=config.active_cards_total_atm_only_source_tag,
        fecha_inicio=config.start_date.strftime("%Y%m%d"),
        fecha_fin=fecha_fin,
        dataset_code=config.active_cards_total_atm_only_dataset_code,
    )
    cards_with_operations_debit = await fetch_card_count_observations(
        client,
        endpoint_base=config.source_endpoint_base,
        tag=config.cards_with_operations_debit_source_tag,
        fecha_inicio=config.start_date.strftime("%Y%m%d"),
        fecha_fin=fecha_fin,
        dataset_code=config.cards_with_operations_debit_dataset_code,
    )
    cards_with_operations_atm_only = await fetch_card_count_observations(
        client,
        endpoint_base=config.source_endpoint_base,
        tag=config.cards_with_operations_atm_only_source_tag,
        fecha_inicio=config.start_date.strftime("%Y%m%d"),
        fecha_fin=fecha_fin,
        dataset_code=config.cards_with_operations_atm_only_dataset_code,
    )

    raw_observations = [
        *to_card_count_raw_observations(active_primary_debit),
        *to_card_count_raw_observations(active_primary_atm_only),
        *to_card_count_raw_observations(active_supplementary_debit),
        *to_card_count_raw_observations(active_supplementary_atm_only),
        *to_card_count_raw_observations(active_total_debit),
        *to_card_count_raw_observations(active_total_atm_only),
        *to_card_count_raw_observations(cards_with_operations_debit),
        *to_card_count_raw_observations(cards_with_operations_atm_only),
    ]

    all_periods = (
        *(observation.period_month for observation in active_primary_debit),
        *(observation.period_month for observation in active_primary_atm_only),
        *(observation.period_month for observation in active_supplementary_debit),
        *(observation.period_month for observation in active_supplementary_atm_only),
        *(observation.period_month for observation in active_total_debit),
        *(observation.period_month for observation in active_total_atm_only),
        *(observation.period_month for observation in cards_with_operations_debit),
        *(observation.period_month for observation in cards_with_operations_atm_only),
    )

    return BankDebitCardCountsObservationBatch(
        raw_observations=sorted(
            raw_observations,
            key=lambda observation: (
                observation.dataset_code,
                observation.institution_code,
                observation.period_month,
            ),
        ),
        latest_source_month=max(all_periods, default=None),
        earliest_source_month=min(all_periods, default=None),
        latest_active_cards_primary_debit_source_month=max((observation.period_month for observation in active_primary_debit), default=None),
        latest_active_cards_primary_atm_only_source_month=max((observation.period_month for observation in active_primary_atm_only), default=None),
        latest_active_cards_supplementary_debit_source_month=max((observation.period_month for observation in active_supplementary_debit), default=None),
        latest_active_cards_supplementary_atm_only_source_month=max((observation.period_month for observation in active_supplementary_atm_only), default=None),
        latest_active_cards_total_debit_source_month=max((observation.period_month for observation in active_total_debit), default=None),
        latest_active_cards_total_atm_only_source_month=max((observation.period_month for observation in active_total_atm_only), default=None),
        latest_cards_with_operations_debit_source_month=max((observation.period_month for observation in cards_with_operations_debit), default=None),
        latest_cards_with_operations_atm_only_source_month=max((observation.period_month for observation in cards_with_operations_atm_only), default=None),
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

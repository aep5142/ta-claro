from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlencode

from data.models.cmf_cards import (
    BANK_CREDIT_CARD_PURCHASE_VOLUME_DATASET,
    BANK_CREDIT_CARD_PURCHASE_VOLUME_TAG,
    BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET,
    BANK_CREDIT_CARD_TRANSACTION_COUNT_TAG,
    CMF_CARDS_START_DATE,
    CmfPurchaseVolumeRawObservation,
    CmfTransactionCountRawObservation,
)


def build_cmf_cuadros_url(
    *,
    endpoint_base: str,
    tag: str,
    fecha_fin: date,
    fecha_inicio: str = CMF_CARDS_START_DATE,
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


def normalize_period_month(raw_period: str | date) -> date:
    if isinstance(raw_period, date):
        return raw_period.replace(day=1)

    for date_format in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
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
    dataset_code: str = BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET,
) -> list[CmfTransactionCountRawObservation]:
    observations: list[CmfTransactionCountRawObservation] = []

    for series in _get_series(payload):
        source_codigo = _first_present(series, "Codigo", "codigo", "source_codigo")
        institution_code = derive_institution_code(source_codigo)
        source_nombre = _first_present(series, "Nombre", "nombre", "source_nombre")
        source_series_id = str(_first_present(series, "id", "Id", "source_series_id"))

        for point in _get_observations(series):
            observations.append(
                CmfTransactionCountRawObservation(
                    dataset_code=dataset_code,
                    source_series_id=source_series_id,
                    source_codigo=source_codigo,
                    source_nombre=source_nombre,
                    institution_code=institution_code,
                    institution_name=source_nombre,
                    period_month=normalize_period_month(
                        _first_present(point, "Fecha", "fecha", "period", "Periodo")
                    ),
                    transaction_count=parse_cmf_numeric(
                        _first_present(point, "Valor", "valor", "value")
                    ),
                    source_payload=point,
                )
            )

    return sorted(
        observations,
        key=lambda observation: (
            observation.institution_code,
            observation.period_month,
        ),
    )


def parse_purchase_volume_payload(
    payload: dict[str, Any],
    *,
    dataset_code: str = BANK_CREDIT_CARD_PURCHASE_VOLUME_DATASET,
) -> list[CmfPurchaseVolumeRawObservation]:
    observations: list[CmfPurchaseVolumeRawObservation] = []

    for series in _get_series(payload):
        source_codigo = _first_present(series, "Codigo", "codigo", "source_codigo")
        institution_code = derive_institution_code(source_codigo)
        source_nombre = _first_present(series, "Nombre", "nombre", "source_nombre")
        source_series_id = str(_first_present(series, "id", "Id", "source_series_id"))

        for point in _get_observations(series):
            observations.append(
                CmfPurchaseVolumeRawObservation(
                    dataset_code=dataset_code,
                    source_series_id=source_series_id,
                    source_codigo=source_codigo,
                    source_nombre=source_nombre,
                    institution_code=institution_code,
                    institution_name=source_nombre,
                    period_month=normalize_period_month(
                        _first_present(point, "Fecha", "fecha", "period", "Periodo")
                    ),
                    nominal_volume_clp=parse_cmf_numeric(
                        _first_present(point, "Valor", "valor", "value")
                    ),
                    source_payload=point,
                )
            )

    return sorted(
        observations,
        key=lambda observation: (
            observation.institution_code,
            observation.period_month,
        ),
    )


async def fetch_transaction_count_payload(
    client,
    *,
    endpoint_base: str,
    fecha_fin: date,
):
    response = await client.get(
        build_cmf_cuadros_url(
            endpoint_base=endpoint_base,
            tag=BANK_CREDIT_CARD_TRANSACTION_COUNT_TAG,
            fecha_fin=fecha_fin,
        ),
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


async def fetch_purchase_volume_payload(
    client,
    *,
    endpoint_base: str,
    fecha_fin: date,
):
    response = await client.get(
        build_cmf_cuadros_url(
            endpoint_base=endpoint_base,
            tag=BANK_CREDIT_CARD_PURCHASE_VOLUME_TAG,
            fecha_fin=fecha_fin,
        ),
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


async def fetch_transaction_count_observations(
    client,
    *,
    endpoint_base: str,
    fecha_fin: date,
) -> list[CmfTransactionCountRawObservation]:
    payload = await fetch_transaction_count_payload(
        client,
        endpoint_base=endpoint_base,
        fecha_fin=fecha_fin,
    )
    return parse_transaction_count_payload(payload)


async def fetch_purchase_volume_observations(
    client,
    *,
    endpoint_base: str,
    fecha_fin: date,
) -> list[CmfPurchaseVolumeRawObservation]:
    payload = await fetch_purchase_volume_payload(
        client,
        endpoint_base=endpoint_base,
        fecha_fin=fecha_fin,
    )
    return parse_purchase_volume_payload(payload)


def _get_series(payload: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("series", "Series"):
        if key in payload and isinstance(payload[key], list):
            return payload[key]

    raise ValueError("CMF payload does not include a series list")


def _get_observations(series: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("data", "datos", "Datos", "observations", "valores", "Valores"):
        if key in series and isinstance(series[key], list):
            return series[key]

    raise ValueError("CMF series does not include observations")


def _first_present(source: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = source.get(key)
        if value is not None:
            return value

    raise ValueError(f"Missing required CMF field. Tried: {', '.join(keys)}")

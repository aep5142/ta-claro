from datetime import date
from decimal import Decimal

import pytest

from data.models.bank_credit_card_operations import (
    BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_DATASET,
    BANK_CREDIT_CARD_OPS_CARGOS_POR_SERVICIO_DATASET,
    BANK_CREDIT_CARD_OPS_COMPRAS_DATASET,
)
from data.sources.bank_credit_card_operations import (
    build_cmf_cuadros_url,
    derive_institution_code,
    merge_operation_measure_observations,
    normalize_period_month,
    parse_cmf_numeric,
    parse_nominal_volume_payload,
    parse_transaction_count_payload,
)


def _payload(*, series_id: int, codigo: str, nombre: str, values: list[tuple[str, str]]):
    return {
        "series": [
            {
                "id": series_id,
                "Codigo": codigo,
                "Nombre": nombre,
                "data": [
                    {"Fecha": fecha, "Valor": valor}
                    for fecha, valor in values
                ],
            }
        ]
    }


def test_build_cmf_cuadros_url_for_new_operation_endpoint():
    url = build_cmf_cuadros_url(
        endpoint_base="https://best-sbif-api.azurewebsites.net/Cuadrosv2",
        tag="SBIF_TCRED_BANC_AVEF_AGIFI_NUM",
        fecha_fin=date(2026, 4, 24),
        fecha_inicio="20090401",
    )

    assert url == (
        "https://best-sbif-api.azurewebsites.net/Cuadrosv2?"
        "FechaFin=20260424&FechaInicio=20090401&"
        "Tag=SBIF_TCRED_BANC_AVEF_AGIFI_NUM&from=reload"
    )


@pytest.mark.parametrize(
    ("source_codigo", "expected_code"),
    [
        ("SBIF_TCRED_BANC_AVEF_AGIFI_BICE_NUM", "BICE"),
        ("SBIF_TCRED_BANC_CSERV_AGIFI_BICE_$", "BICE"),
    ],
)
def test_derive_institution_code_from_new_operation_source_codes(
    source_codigo,
    expected_code,
):
    assert derive_institution_code(source_codigo) == expected_code


def test_derive_institution_code_rejects_missing_agifi_token():
    with pytest.raises(ValueError, match="Cannot derive institution_code"):
        derive_institution_code("SBIF_TCRED_BANC_OPER_049_NUM")


def test_parse_cmf_numeric_accepts_string_values():
    assert parse_cmf_numeric("1.234.567") == Decimal("1234567")
    assert parse_cmf_numeric("1234,50") == Decimal("1234.50")


def test_normalize_period_month_accepts_common_source_formats():
    assert normalize_period_month("2026-04-24") == date(2026, 4, 1)
    assert normalize_period_month("24-04-2026") == date(2026, 4, 1)
    assert normalize_period_month("202604") == date(2026, 4, 1)
    assert normalize_period_month("2026-04") == date(2026, 4, 1)


def test_parse_transaction_count_payload_normalizes_source_observations():
    payload = _payload(
        series_id=101,
        codigo="SBIF_TCRED_BANC_AVEF_AGIFI_BICE_NUM",
        nombre="Banco BICE",
        values=[("2026-03-01", "1.234"), ("2026-04-01", "2.500")],
    )

    observations = parse_transaction_count_payload(
        payload,
        operation_type="Avance en Efectivo",
        dataset_code=BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_DATASET,
    )

    assert len(observations) == 2
    assert observations[0].dataset_code == BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_DATASET
    assert observations[0].source_series_id == "101"
    assert observations[0].source_codigo == "SBIF_TCRED_BANC_AVEF_AGIFI_BICE_NUM"
    assert observations[0].institution_code == "BICE"
    assert observations[0].institution_name == "Banco BICE"
    assert observations[0].period_month == date(2026, 3, 1)
    assert observations[0].value == Decimal("1234")
    assert observations[0].source_payload == {"Fecha": "2026-03-01", "Valor": "1.234"}


def test_parse_nominal_volume_payload_normalizes_source_observations():
    payload = _payload(
        series_id=301,
        codigo="SBIF_TCRED_BANC_CSERV_AGIFI_BICE_$",
        nombre="Banco BICE",
        values=[("2026-03-01", "1.000.000"), ("2026-04-01", "2.500.000")],
    )

    observations = parse_nominal_volume_payload(
        payload,
        operation_type="Cargos por Servicio",
        dataset_code=BANK_CREDIT_CARD_OPS_CARGOS_POR_SERVICIO_DATASET,
    )

    assert len(observations) == 2
    assert observations[0].dataset_code == BANK_CREDIT_CARD_OPS_CARGOS_POR_SERVICIO_DATASET
    assert observations[0].source_series_id == "301"
    assert observations[0].source_codigo == "SBIF_TCRED_BANC_CSERV_AGIFI_BICE_$"
    assert observations[0].institution_code == "BICE"
    assert observations[0].institution_name == "Banco BICE"
    assert observations[0].period_month == date(2026, 3, 1)
    assert observations[0].value == Decimal("1000000")
    assert observations[0].source_payload == {"Fecha": "2026-03-01", "Valor": "1.000.000"}


def test_merge_operation_measure_observations_builds_raw_rows():
    transaction_count_observations = parse_transaction_count_payload(
        _payload(
            series_id=101,
            codigo="SBIF_TCRED_BANC_COMP_AGIFI_BICE_NUM",
            nombre="Banco BICE",
            values=[("2026-04-01", "2.500")],
        ),
        operation_type="Compras",
        dataset_code=BANK_CREDIT_CARD_OPS_COMPRAS_DATASET,
    )
    nominal_volume_observations = parse_nominal_volume_payload(
        _payload(
            series_id=301,
            codigo="SBIF_TCRED_BANC_COMP_AGIFI_BICE_$",
            nombre="Banco BICE",
            values=[("2026-04-01", "120.507.338")],
        ),
        operation_type="Compras",
        dataset_code=BANK_CREDIT_CARD_OPS_COMPRAS_DATASET,
    )

    observations = merge_operation_measure_observations(
        operation_type="Compras",
        dataset_code=BANK_CREDIT_CARD_OPS_COMPRAS_DATASET,
        transaction_count_observations=transaction_count_observations,
        nominal_volume_observations=nominal_volume_observations,
    )

    assert len(observations) == 1
    assert observations[0].operation_type == "Compras"
    assert observations[0].dataset_code == BANK_CREDIT_CARD_OPS_COMPRAS_DATASET
    assert observations[0].transaction_count == Decimal("2500")
    assert observations[0].nominal_volume_millions_clp == Decimal("120507338")
    assert observations[0].source_payload == {
        "transaction_count": {"Fecha": "2026-04-01", "Valor": "2.500"},
        "nominal_volume_millions_clp": {
            "Fecha": "2026-04-01",
            "Valor": "120.507.338",
        },
    }

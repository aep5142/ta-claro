from datetime import date
from decimal import Decimal

import pytest

from data.models.bank_credit_card_operations import BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET
from data.sources.bank_credit_card_operations import (
    build_cmf_cuadros_url,
    derive_institution_code,
    normalize_period_month,
    parse_cmf_numeric,
    parse_purchase_volume_payload,
    parse_transaction_count_payload,
)
from tests.fixtures.bank_credit_card_operations_live_payload import (
    LIVE_PURCHASE_VOLUME_PAYLOAD,
    LIVE_TRANSACTION_COUNT_PAYLOAD,
)
from tests.fixtures.cmf_purchase_volume_payload import PURCHASE_VOLUME_PAYLOAD
from tests.fixtures.cmf_transaction_count_payload import TRANSACTION_COUNT_PAYLOAD


def test_build_cmf_cuadros_url_for_transaction_count_endpoint():
    url = build_cmf_cuadros_url(
        endpoint_base="https://best-sbif-api.azurewebsites.net/Cuadrosv2",
        tag="SBIF_TCRED_BANC_COMP_AGIFI_NUM",
        fecha_fin=date(2026, 4, 24),
    )

    assert url == (
        "https://best-sbif-api.azurewebsites.net/Cuadrosv2?"
        "FechaFin=20260424&FechaInicio=20090401&"
        "Tag=SBIF_TCRED_BANC_COMP_AGIFI_NUM&from=reload"
    )


def test_derive_institution_code_from_source_codigo():
    assert derive_institution_code("SBIF_TCRED_BANC_COMP_AGIFI_049_NUM") == "049"


def test_derive_institution_code_rejects_missing_agifi_token():
    with pytest.raises(ValueError, match="Cannot derive institution_code"):
        derive_institution_code("SBIF_TCRED_BANC_COMP_049_NUM")


def test_parse_cmf_numeric_accepts_string_values():
    assert parse_cmf_numeric("1.234.567") == Decimal("1234567")
    assert parse_cmf_numeric("1234,50") == Decimal("1234.50")


def test_normalize_period_month_accepts_common_source_formats():
    assert normalize_period_month("2026-04-24") == date(2026, 4, 1)
    assert normalize_period_month("24-04-2026") == date(2026, 4, 1)
    assert normalize_period_month("202604") == date(2026, 4, 1)
    assert normalize_period_month("2026-04") == date(2026, 4, 1)


def test_parse_transaction_count_payload_normalizes_source_observations():
    observations = parse_transaction_count_payload(TRANSACTION_COUNT_PAYLOAD)

    assert len(observations) == 3
    assert observations[0].dataset_code == BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET
    assert observations[0].source_series_id == "101"
    assert observations[0].source_codigo == "SBIF_TCRED_BANC_COMP_AGIFI_001_NUM"
    assert observations[0].institution_code == "001"
    assert observations[0].institution_name == "Banco Uno"
    assert observations[0].period_month == date(2026, 3, 1)
    assert observations[0].transaction_count == Decimal("1234")
    assert observations[0].source_payload == {"Fecha": "2026-03-01", "Valor": "1.234"}


def test_parse_purchase_volume_payload_normalizes_nominal_clp_observations():
    observations = parse_purchase_volume_payload(PURCHASE_VOLUME_PAYLOAD)

    assert len(observations) == 3
    assert observations[0].source_series_id == "301"
    assert observations[0].source_codigo == "SBIF_TCRED_BANC_COMP_AGIFI_001_$"
    assert observations[0].institution_code == "001"
    assert observations[0].institution_name == "Banco Uno"
    assert observations[0].period_month == date(2026, 3, 1)
    assert observations[0].nominal_volume_millions_clp == Decimal("1000000")
    assert observations[0].source_payload == {
        "Fecha": "2026-03-01",
        "Valor": "1.000.000",
    }


def test_parse_transaction_count_payload_accepts_live_cmf_shape():
    observations = parse_transaction_count_payload(LIVE_TRANSACTION_COUNT_PAYLOAD)

    assert len(observations) == 2
    assert observations[0].institution_name == "Banco BICE"
    assert observations[0].source_nombre == "Banco BICE"
    assert observations[0].period_month == date(2025, 12, 1)
    assert observations[0].transaction_count == Decimal("1980183")
    assert observations[0].source_payload == {"fecha": 20251201, "valor": 1980183.0}
    assert observations[1].period_month == date(2026, 1, 1)
    assert observations[1].transaction_count == Decimal("1798085")
    assert observations[1].source_payload == {"fecha": 20260101, "valor": 1798085.0}


def test_parse_purchase_volume_payload_accepts_live_cmf_shape():
    observations = parse_purchase_volume_payload(LIVE_PURCHASE_VOLUME_PAYLOAD)

    assert len(observations) == 2
    assert observations[0].institution_name == "Banco BICE"
    assert observations[0].source_nombre == "Banco BICE"
    assert observations[0].period_month == date(2025, 12, 1)
    assert observations[0].nominal_volume_millions_clp == Decimal("137211.788458")
    assert observations[0].source_payload == {"fecha": 20251201, "valor": 137211.788458}
    assert observations[1].period_month == date(2026, 1, 1)
    assert observations[1].nominal_volume_millions_clp == Decimal("120507.338158")
    assert observations[1].source_payload == {"fecha": 20260101, "valor": 120507.338158}

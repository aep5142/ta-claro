from datetime import date
from decimal import Decimal

import pytest

from data.loaders.cmf_purchase_volume_loader import (
    get_uf_value_for_date,
    latest_curated_purchase_volume_month,
    upsert_purchase_volume_curated,
    upsert_purchase_volume_raw,
)
from data.models.bank_credit_card_operations import (
    BANK_CREDIT_CARD_PURCHASE_VOLUME_DATASET,
    CmfPurchaseVolumeCuratedObservation,
    CmfPurchaseVolumeRawObservation,
)


class FakeResponse:
    def __init__(self, data=None):
        self.data = data or []


class FakeTable:
    def __init__(self, name, db):
        self.name = name
        self.db = db
        self._upsert_payload = None
        self._upsert_kwargs = None
        self._selected = False
        self._eq_filter = None

    def select(self, *_args):
        self._selected = True
        return self

    def order(self, *_args, **_kwargs):
        return self

    def eq(self, column, value):
        self._eq_filter = (column, value)
        return self

    def limit(self, *_args):
        return self

    def upsert(self, payload, **kwargs):
        self._upsert_payload = payload
        self._upsert_kwargs = kwargs
        return self

    def execute(self):
        if self._upsert_payload is not None:
            self.db["upserts"].append(
                {
                    "table": self.name,
                    "payload": self._upsert_payload,
                    "kwargs": self._upsert_kwargs,
                }
            )
            return FakeResponse()

        if self.name == "uf_values":
            return FakeResponse(self.db["uf_values"].get(self._eq_filter[1], []))

        return FakeResponse(self.db["latest_curated"])


class FakeSupabase:
    def __init__(self, latest_curated=None, uf_values=None):
        self.db = {
            "latest_curated": latest_curated or [],
            "uf_values": uf_values or {},
            "upserts": [],
        }

    def table(self, name):
        return FakeTable(name, self.db)

    @property
    def upserts(self):
        return self.db["upserts"]


def _raw_observation():
    return CmfPurchaseVolumeRawObservation(
        dataset_code=BANK_CREDIT_CARD_PURCHASE_VOLUME_DATASET,
        source_series_id="301",
        source_codigo="SBIF_TCRED_BANC_COMP_AGIFI_001_$",
        source_nombre="Banco Uno",
        institution_code="001",
        institution_name="Banco Uno",
        period_month=date(2026, 4, 1),
        nominal_volume_clp=Decimal("1000000"),
        source_payload={"Fecha": "2026-04-01", "Valor": "1.000.000"},
    )


def _curated_observation():
    return CmfPurchaseVolumeCuratedObservation(
        institution_code="001",
        institution_name="Banco Uno",
        period_month=date(2026, 4, 1),
        nominal_volume_clp=Decimal("1000000"),
        uf_date_used=date(2026, 4, 15),
        uf_value_used=Decimal("40000"),
        real_volume_uf=Decimal("25"),
        source_dataset_code=BANK_CREDIT_CARD_PURCHASE_VOLUME_DATASET,
    )


def test_latest_curated_purchase_volume_month_returns_none_for_empty_table():
    assert latest_curated_purchase_volume_month(FakeSupabase()) is None


def test_latest_curated_purchase_volume_month_reads_latest_row():
    sb = FakeSupabase(latest_curated=[{"period_month": "2026-04-01"}])

    assert latest_curated_purchase_volume_month(sb) == date(2026, 4, 1)


def test_get_uf_value_for_date_returns_decimal_value():
    sb = FakeSupabase(uf_values={"2026-04-15": [{"value": "40000.25"}]})

    assert get_uf_value_for_date(sb, date(2026, 4, 15)) == Decimal("40000.25")


def test_get_uf_value_for_date_raises_when_missing():
    with pytest.raises(ValueError, match="Missing UF value for 2026-04-15"):
        get_uf_value_for_date(FakeSupabase(), date(2026, 4, 15))


def test_upsert_purchase_volume_raw_uses_idempotent_conflict_key():
    sb = FakeSupabase()

    upsert_purchase_volume_raw(sb, [_raw_observation()])

    assert sb.upserts[0]["table"] == "cmf_card_purchase_volume_raw"
    assert sb.upserts[0]["kwargs"] == {
        "on_conflict": "dataset_code,source_codigo,period_month"
    }
    assert sb.upserts[0]["payload"][0]["nominal_volume_clp"] == "1000000"


def test_upsert_purchase_volume_curated_uses_idempotent_conflict_key():
    sb = FakeSupabase()

    upsert_purchase_volume_curated(sb, [_curated_observation()])

    assert sb.upserts[0]["table"] == "cmf_card_purchase_volume_curated"
    assert sb.upserts[0]["kwargs"] == {
        "on_conflict": "institution_code,period_month"
    }
    assert sb.upserts[0]["payload"][0]["real_volume_uf"] == "25"

from datetime import date
from decimal import Decimal

import pytest

from data.loaders.cmf_purchase_volume_loader import (
    get_uf_value_for_date,
    latest_curated_purchase_volume_month,
    refresh_bank_credit_card_purchases_metrics,
    upsert_purchase_volume_curated,
    upsert_purchase_volume_raw,
)
from data.models.bank_credit_card_operations import (
    BANK_CREDIT_CARD_PURCHASES_METRICS_TABLE,
    BANK_CREDIT_CARD_PURCHASE_VOLUME_DATASET,
    BANK_CREDIT_CARD_PURCHASE_VOLUME_CURATED_TABLE,
    BANK_CREDIT_CARD_PURCHASE_VOLUME_RAW_TABLE,
    BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET,
    BANK_CREDIT_CARD_TRANSACTION_COUNT_CURATED_TABLE,
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

        if self.name == BANK_CREDIT_CARD_PURCHASE_VOLUME_CURATED_TABLE:
            return FakeResponse(
                self.db["purchase_volume_curated"] or self.db["latest_curated"]
            )

        if self.name == BANK_CREDIT_CARD_TRANSACTION_COUNT_CURATED_TABLE:
            return FakeResponse(self.db["transaction_count_curated"])

        return FakeResponse(self.db["latest_curated"])


class FakeSupabase:
    def __init__(
        self,
        latest_curated=None,
        uf_values=None,
        purchase_volume_curated=None,
        transaction_count_curated=None,
    ):
        self.db = {
            "latest_curated": latest_curated or [],
            "uf_values": uf_values or {},
            "purchase_volume_curated": purchase_volume_curated or [],
            "transaction_count_curated": transaction_count_curated or [],
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
        nominal_volume_millions_clp=Decimal("4000"),
        source_payload={"Fecha": "2026-04-01", "Valor": "1.000.000"},
    )


def _curated_observation():
    return CmfPurchaseVolumeCuratedObservation(
        institution_code="001",
        institution_name="Banco Uno",
        period_month=date(2026, 4, 1),
        nominal_volume_thousands_millions_clp=Decimal("4"),
        uf_date_used=date(2026, 4, 15),
        uf_value_used=Decimal("40000"),
        real_volume_uf=Decimal("100000"),
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

    assert sb.upserts[0]["table"] == BANK_CREDIT_CARD_PURCHASE_VOLUME_RAW_TABLE
    assert sb.upserts[0]["kwargs"] == {
        "on_conflict": "dataset_code,source_codigo,period_month"
    }
    assert sb.upserts[0]["payload"][0]["nominal_volume_millions_clp"] == "4000"


def test_upsert_purchase_volume_curated_uses_idempotent_conflict_key():
    sb = FakeSupabase()

    upsert_purchase_volume_curated(sb, [_curated_observation()])

    assert sb.upserts[0]["table"] == BANK_CREDIT_CARD_PURCHASE_VOLUME_CURATED_TABLE
    assert sb.upserts[0]["kwargs"] == {
        "on_conflict": "institution_code,period_month"
    }
    assert sb.upserts[0]["payload"][0]["real_volume_uf"] == "100000"


def test_refresh_bank_credit_card_purchases_metrics_upserts_joined_rows():
    sb = FakeSupabase(
        purchase_volume_curated=[
            {
                "institution_code": "001",
                "institution_name": "Banco Uno",
                "period_month": "2026-04-01",
                "nominal_volume_thousands_millions_clp": "4",
                "uf_date_used": "2026-04-15",
                "uf_value_used": "40000",
                "real_volume_uf": "100000",
                "source_dataset_code": BANK_CREDIT_CARD_PURCHASE_VOLUME_DATASET,
            }
        ],
        transaction_count_curated=[
            {
                "institution_code": "001",
                "institution_name": "Banco Uno",
                "period_month": "2026-04-01",
                "transaction_count": "1000",
                "source_dataset_code": BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET,
            }
        ],
    )

    refresh_bank_credit_card_purchases_metrics(sb)

    assert sb.upserts[0]["table"] == BANK_CREDIT_CARD_PURCHASES_METRICS_TABLE
    assert sb.upserts[0]["kwargs"] == {
        "on_conflict": "institution_code,period_month"
    }
    assert sb.upserts[0]["payload"][0]["average_ticket_uf"] == "100"

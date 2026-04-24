import asyncio
from datetime import date
from decimal import Decimal

import pytest

import data.workers.cmf_purchase_volume_worker as worker
from data.models.cmf_cards import (
    BANK_CREDIT_CARD_PURCHASE_VOLUME_DATASET,
    CmfPurchaseVolumeRawObservation,
)
from data.workers.cmf_purchase_volume_worker import (
    PurchaseVolumeConfig,
    sync_purchase_volume_once,
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
        self._eq_filter = None

    def select(self, *_args):
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


def _raw_observation(period_month):
    return CmfPurchaseVolumeRawObservation(
        dataset_code=BANK_CREDIT_CARD_PURCHASE_VOLUME_DATASET,
        source_series_id="301",
        source_codigo="SBIF_TCRED_BANC_COMP_AGIFI_001_$",
        source_nombre="Banco Uno",
        institution_code="001",
        institution_name="Banco Uno",
        period_month=period_month,
        nominal_volume_clp=Decimal("1000000"),
        source_payload={"Fecha": period_month.isoformat(), "Valor": "1.000.000"},
    )


def test_sync_purchase_volume_once_noops_when_no_new_months(monkeypatch):
    async def fake_fetch_purchase_volume_observations(*_args, **_kwargs):
        return [_raw_observation(date(2026, 4, 1))]

    monkeypatch.setattr(
        worker,
        "fetch_purchase_volume_observations",
        fake_fetch_purchase_volume_observations,
    )
    sb = FakeSupabase(latest_curated=[{"period_month": "2026-04-01"}])

    rows_synced = asyncio.run(
        sync_purchase_volume_once(
            None,
            sb,
            config=PurchaseVolumeConfig(),
            run_date=date(2026, 4, 24),
        )
    )

    assert rows_synced == 0
    assert sb.upserts == []


def test_sync_purchase_volume_once_loads_newer_months_with_uf_enrichment(monkeypatch):
    async def fake_fetch_purchase_volume_observations(*_args, **_kwargs):
        return [
            _raw_observation(date(2026, 3, 1)),
            _raw_observation(date(2026, 4, 1)),
        ]

    monkeypatch.setattr(
        worker,
        "fetch_purchase_volume_observations",
        fake_fetch_purchase_volume_observations,
    )
    sb = FakeSupabase(
        latest_curated=[{"period_month": "2026-03-01"}],
        uf_values={"2026-04-15": [{"value": "40000"}]},
    )

    rows_synced = asyncio.run(
        sync_purchase_volume_once(
            None,
            sb,
            config=PurchaseVolumeConfig(),
            run_date=date(2026, 4, 24),
        )
    )

    assert rows_synced == 1
    assert [upsert["table"] for upsert in sb.upserts] == [
        "cmf_card_purchase_volume_raw",
        "cmf_card_purchase_volume_curated",
    ]
    assert sb.upserts[0]["payload"][0]["period_month"] == "2026-04-01"
    assert sb.upserts[1]["payload"][0]["uf_date_used"] == "2026-04-15"
    assert sb.upserts[1]["payload"][0]["real_volume_uf"] == "25"


def test_sync_purchase_volume_once_fails_when_required_uf_is_missing(monkeypatch):
    async def fake_fetch_purchase_volume_observations(*_args, **_kwargs):
        return [_raw_observation(date(2026, 4, 1))]

    monkeypatch.setattr(
        worker,
        "fetch_purchase_volume_observations",
        fake_fetch_purchase_volume_observations,
    )
    sb = FakeSupabase()

    with pytest.raises(ValueError, match="Missing UF value for 2026-04-15"):
        asyncio.run(
            sync_purchase_volume_once(
                None,
                sb,
                config=PurchaseVolumeConfig(),
                run_date=date(2026, 4, 24),
            )
        )

import asyncio
from datetime import date
from decimal import Decimal

import data.workers.cmf_transaction_count_worker as worker
from data.models.bank_credit_card_operations import (
    BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET,
    BANK_CREDIT_CARD_TRANSACTION_COUNT_CURATED_TABLE,
    BANK_CREDIT_CARD_TRANSACTION_COUNT_RAW_TABLE,
    CmfTransactionCountRawObservation,
)
from data.workers.cmf_transaction_count_worker import (
    TransactionCountConfig,
    sync_transaction_count_once,
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

    def select(self, *_args):
        return self

    def order(self, *_args, **_kwargs):
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

        return FakeResponse(self.db["latest_curated"])


class FakeSupabase:
    def __init__(self, latest_curated=None):
        self.db = {
            "latest_curated": latest_curated or [],
            "upserts": [],
        }

    def table(self, name):
        return FakeTable(name, self.db)

    @property
    def upserts(self):
        return self.db["upserts"]


def _raw_observation(period_month):
    return CmfTransactionCountRawObservation(
        dataset_code=BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET,
        source_series_id="101",
        source_codigo="SBIF_TCRED_BANC_COMP_AGIFI_001_NUM",
        source_nombre="Banco Uno",
        institution_code="001",
        institution_name="Banco Uno",
        period_month=period_month,
        transaction_count=Decimal("1234"),
        source_payload={"Fecha": period_month.isoformat(), "Valor": "1.234"},
    )


def test_sync_transaction_count_once_noops_when_no_new_months(monkeypatch):
    async def fake_fetch_transaction_count_observations(*_args, **_kwargs):
        return [_raw_observation(date(2026, 4, 1))]

    monkeypatch.setattr(
        worker,
        "fetch_transaction_count_observations",
        fake_fetch_transaction_count_observations,
    )
    sb = FakeSupabase(latest_curated=[{"period_month": "2026-04-01"}])

    rows_synced = asyncio.run(
        sync_transaction_count_once(
            None,
            sb,
            config=TransactionCountConfig(),
            run_date=date(2026, 4, 24),
        )
    )

    assert rows_synced == 0
    assert sb.upserts == []


def test_sync_transaction_count_once_loads_only_newer_months(monkeypatch):
    async def fake_fetch_transaction_count_observations(*_args, **_kwargs):
        return [
            _raw_observation(date(2026, 3, 1)),
            _raw_observation(date(2026, 4, 1)),
        ]

    monkeypatch.setattr(
        worker,
        "fetch_transaction_count_observations",
        fake_fetch_transaction_count_observations,
    )
    sb = FakeSupabase(latest_curated=[{"period_month": "2026-03-01"}])

    rows_synced = asyncio.run(
        sync_transaction_count_once(
            None,
            sb,
            config=TransactionCountConfig(),
            run_date=date(2026, 4, 24),
        )
    )

    assert rows_synced == 1
    assert [upsert["table"] for upsert in sb.upserts] == [
        BANK_CREDIT_CARD_TRANSACTION_COUNT_RAW_TABLE,
        BANK_CREDIT_CARD_TRANSACTION_COUNT_CURATED_TABLE,
    ]
    assert sb.upserts[0]["payload"][0]["period_month"] == "2026-04-01"
    assert sb.upserts[1]["payload"][0]["period_month"] == "2026-04-01"

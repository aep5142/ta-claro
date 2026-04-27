import asyncio
from datetime import date
from decimal import Decimal

import pytest

from data.models.bank_credit_card_operations import (
    BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_DATASET,
    BANK_CREDIT_CARD_OPS_COMPRAS_DATASET,
    BankCreditCardOperationConfig,
    BankCreditCardOpsRawObservation,
)
from data.sources.bank_credit_card_operations import BankCreditCardOpsObservationBatch
from data.workers.bank_credit_card_ops_worker import (
    BankCreditCardOpsWorkerConfig,
    load_active_operation_configs,
    sync_all_bank_credit_card_ops_once,
    sync_operation_once,
)


class FakeResponse:
    def __init__(self, data=None):
        self.data = data or []


class FakeTable:
    def __init__(self, name, db):
        self.name = name
        self.db = db
        self._eq_filter = None
        self._upsert_payload = None
        self._upsert_kwargs = None

    def select(self, *_args):
        return self

    def eq(self, column, value):
        self._eq_filter = (column, value)
        return self

    def limit(self, *_args):
        return self

    def order(self, *_args, **_kwargs):
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

        if self.name == "cmf_datasets":
            return FakeResponse(
                [
                    row
                    for row in self.db["datasets"]
                    if self._eq_filter is None or row[self._eq_filter[0]] == self._eq_filter[1]
                ]
            )

        if self.name == "cmf_dataset_sync_state":
            return FakeResponse(self.db["states"].get(self._eq_filter[1], []))

        if self.name == "uf_values":
            return FakeResponse(self.db["uf_values"].get(self._eq_filter[1], []))

        return FakeResponse(self.db.get(self.name, []))


class FakeSupabase:
    def __init__(self, datasets=None, states=None, uf_values=None):
        self.db = {
            "datasets": datasets or [],
            "states": states or {},
            "uf_values": uf_values or {},
            "upserts": [],
        }

    def table(self, name):
        return FakeTable(name, self.db)

    @property
    def upserts(self):
        return self.db["upserts"]


def _config(dataset_code: str, operation_type: str) -> BankCreditCardOperationConfig:
    return BankCreditCardOperationConfig(
        operation_type=operation_type,
        dataset_code=dataset_code,
        transaction_count_source_tag="count-tag",
        nominal_volume_source_tag="volume-tag",
        source_nombre=operation_type,
        source_description=f"{operation_type} description",
        source_endpoint_base="https://cmf.example",
        refresh_frequency="monthly",
        start_date=date(2009, 4, 1),
    )


def _batch(dataset_code: str, operation_type: str, period_month: date, rows_synced: int = 1):
    raw_observations = [
        BankCreditCardOpsRawObservation(
            operation_type=operation_type,
            dataset_code=dataset_code,
            source_series_id="301",
            source_codigo="SBIF_TCRED_BANC_COMP_AGIFI_BICE_$",
            source_nombre="Banco BICE",
            institution_code="BICE",
            institution_name="Banco BICE",
            period_month=period_month,
            transaction_count=Decimal("2500"),
            nominal_volume_millions_clp=Decimal("120507338"),
            source_payload={"transaction_count": {}, "nominal_volume_millions_clp": {}},
        )
        for _ in range(rows_synced)
    ]

    return BankCreditCardOpsObservationBatch(
        raw_observations=raw_observations,
        latest_source_month=period_month,
    )


def test_load_active_operation_configs_reads_registry_rows():
    sb = FakeSupabase(
        datasets=[
            {
                "operation_type": "Compras",
                "dataset_code": BANK_CREDIT_CARD_OPS_COMPRAS_DATASET,
                "transaction_count_source_tag": "count-tag",
                "nominal_volume_source_tag": "volume-tag",
                "source_nombre": "Compras",
                "source_description": "Compras desc",
                "source_endpoint_base": "https://cmf.example",
                "refresh_frequency": "monthly",
                "start_date": "2009-04-01",
                "is_active": True,
            },
            {
                "operation_type": "Avance en Efectivo",
                "dataset_code": BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_DATASET,
                "transaction_count_source_tag": "count-tag-2",
                "nominal_volume_source_tag": "volume-tag-2",
                "source_nombre": "Avance en Efectivo",
                "source_description": "Avance desc",
                "source_endpoint_base": "https://cmf.example",
                "refresh_frequency": "monthly",
                "start_date": "2009-04-01",
                "is_active": True,
            },
        ]
    )

    configs = load_active_operation_configs(sb)

    assert [config.dataset_code for config in configs] == [
        BANK_CREDIT_CARD_OPS_COMPRAS_DATASET,
        BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_DATASET,
    ]


def test_sync_operation_once_noops_when_source_month_is_unchanged(monkeypatch):
    dataset = _config(BANK_CREDIT_CARD_OPS_COMPRAS_DATASET, "Compras")
    sb = FakeSupabase(
        states={BANK_CREDIT_CARD_OPS_COMPRAS_DATASET: [{"latest_source_month": "2026-04-01"}]},
        uf_values={"2026-04-15": [{"value": "40000"}]},
    )

    async def fake_fetch_operation_batch(_client, *, config, fecha_fin):
        assert config.dataset_code == BANK_CREDIT_CARD_OPS_COMPRAS_DATASET
        return _batch(dataset.dataset_code, dataset.operation_type, date(2026, 4, 1))

    monkeypatch.setattr(
        "data.workers.bank_credit_card_ops_worker.fetch_operation_batch",
        fake_fetch_operation_batch,
    )

    rows_synced = asyncio.run(
        sync_operation_once(
            None,
            sb,
            config=dataset,
            run_date=date(2026, 4, 24),
        )
    )

    assert rows_synced == 0
    assert len(sb.upserts) == 1
    assert "last_attempted_sync_at" in sb.upserts[0]["payload"]


def test_sync_operation_once_syncs_newer_source_and_advances_state(monkeypatch):
    dataset = _config(BANK_CREDIT_CARD_OPS_COMPRAS_DATASET, "Compras")
    sb = FakeSupabase(
        states={BANK_CREDIT_CARD_OPS_COMPRAS_DATASET: [{"latest_source_month": "2026-03-01"}]},
        uf_values={"2026-04-15": [{"value": "40000"}]},
    )

    async def fake_fetch_operation_batch(_client, *, config, fecha_fin):
        return _batch(config.dataset_code, config.operation_type, date(2026, 4, 1))

    monkeypatch.setattr(
        "data.workers.bank_credit_card_ops_worker.fetch_operation_batch",
        fake_fetch_operation_batch,
    )

    rows_synced = asyncio.run(
        sync_operation_once(
            None,
            sb,
            config=dataset,
            run_date=date(2026, 4, 24),
        )
    )

    assert rows_synced == 1
    assert sb.upserts[1]["table"] == "bank_credit_card_ops_raw"
    assert sb.upserts[1]["payload"][0]["transaction_count"] == "2500"
    assert sb.upserts[2]["table"] == "bank_credit_card_ops_curated"
    assert sb.upserts[2]["payload"][0]["average_ticket_uf"] == "1205073.38000000000"
    assert sb.upserts[3]["table"] == "cmf_dataset_sync_state"
    assert sb.upserts[3]["payload"]["latest_source_month"] == "2026-04-01"
    assert sb.upserts[3]["payload"]["latest_curated_month"] == "2026-04-01"
    assert sb.upserts[3]["payload"]["last_error"] is None


def test_sync_operation_once_records_failure_without_advancing_state(monkeypatch):
    dataset = _config(BANK_CREDIT_CARD_OPS_COMPRAS_DATASET, "Compras")
    sb = FakeSupabase(
        states={BANK_CREDIT_CARD_OPS_COMPRAS_DATASET: [{"latest_source_month": "2026-03-01"}]},
    )

    async def fake_fetch_operation_batch(_client, *, config, fecha_fin):
        raise RuntimeError("load failed")

    monkeypatch.setattr(
        "data.workers.bank_credit_card_ops_worker.fetch_operation_batch",
        fake_fetch_operation_batch,
    )

    with pytest.raises(RuntimeError, match="load failed"):
        asyncio.run(
            sync_operation_once(
                None,
                sb,
                config=dataset,
                run_date=date(2026, 4, 24),
            )
        )

    assert sb.upserts[-1]["payload"]["last_error"] == "RuntimeError: load failed"
    assert "latest_source_month" not in sb.upserts[-1]["payload"]
    assert "latest_curated_month" not in sb.upserts[-1]["payload"]


def test_sync_all_bank_credit_card_ops_once_continues_after_one_operation_failure(monkeypatch):
    configs = [
        _config(BANK_CREDIT_CARD_OPS_COMPRAS_DATASET, "Compras"),
        _config(BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_DATASET, "Avance en Efectivo"),
    ]
    sb = FakeSupabase()

    async def fake_sync_operation_once(_client, _sb, *, config, run_date):
        if config.dataset_code == BANK_CREDIT_CARD_OPS_COMPRAS_DATASET:
            raise RuntimeError("source unavailable")
        return 3

    monkeypatch.setattr(
        "data.workers.bank_credit_card_ops_worker.sync_operation_once",
        fake_sync_operation_once,
    )

    results = asyncio.run(
        sync_all_bank_credit_card_ops_once(
            None,
            sb,
            config=BankCreditCardOpsWorkerConfig(
                supabase_url="https://supabase.example",
                supabase_service_role_key="service-role",
                endpoint_base="https://cmf.example",
            ),
            run_date=date(2026, 4, 24),
            operations=configs,
        )
    )

    assert results == {
        BANK_CREDIT_CARD_OPS_COMPRAS_DATASET: 0,
        BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_DATASET: 3,
    }

import asyncio
from datetime import date
from decimal import Decimal

import pytest

from data.models.bank_credit_card_operations import (
    BANK_CREDIT_CARD_ACTIVE_CARDS_PRIMARY_DATASET,
    BANK_CREDIT_CARD_ACTIVE_CARDS_SUPPLEMENTARY_DATASET,
    BANK_CREDIT_CARD_CARDS_WITH_OPERATIONS_PRIMARY_DATASET,
    BANK_CREDIT_CARD_CARDS_WITH_OPERATIONS_SUPPLEMENTARY_DATASET,
    BANK_CREDIT_CARD_COUNTS_DATASET,
    BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_DATASET,
    BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_NOMINAL_VOLUME_DATASET,
    BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_TRANSACTION_COUNT_DATASET,
    BANK_CREDIT_CARD_OPS_COMPRAS_DATASET,
    BANK_CREDIT_CARD_OPS_COMPRAS_NOMINAL_VOLUME_DATASET,
    BANK_CREDIT_CARD_OPS_COMPRAS_TRANSACTION_COUNT_DATASET,
    CMF_MEASURE_KIND_ACTIVE_CARDS_PRIMARY,
    CMF_MEASURE_KIND_ACTIVE_CARDS_SUPPLEMENTARY,
    CMF_MEASURE_KIND_CARDS_WITH_OPERATIONS_PRIMARY,
    CMF_MEASURE_KIND_CARDS_WITH_OPERATIONS_SUPPLEMENTARY,
    CMF_MEASURE_KIND_NOMINAL_VOLUME,
    CMF_MEASURE_KIND_TRANSACTION_COUNT,
    BankCreditCardCountsConfig,
    BankCreditCardCountRawObservation,
    BankCreditCardOperationConfig,
    BankCreditCardOpsRawObservation,
)
from data.sources.bank_credit_card_operations import (
    BankCreditCardCountsObservationBatch,
    BankCreditCardOpsObservationBatch,
)
from data.workers.bank_credit_card_ops_worker import (
    BankCreditCardOpsWorkerConfig,
    build_active_cards_lookup,
    load_active_card_counts_config,
    load_active_operation_configs,
    sync_card_counts_once,
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
        self._range = None
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

    def range(self, start, end):
        self._range = (start, end)
        self.db.setdefault("ranges", []).append({"table": self.name, "start": start, "end": end})
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

        if self.name == "bank_credit_card_counts_curated":
            rows = list(self.db.get("bank_credit_card_counts_curated", []))
            if self._eq_filter is not None:
                column, value = self._eq_filter
                rows = [row for row in rows if row.get(column) == value]
            if self._range is not None:
                start, end = self._range
                rows = rows[start : end + 1]
            return FakeResponse(rows)

        return FakeResponse(self.db.get(self.name, []))


class FakeSupabase:
    def __init__(
        self,
        datasets=None,
        states=None,
        uf_values=None,
        count_curated=None,
        ops_curated=None,
    ):
        self.db = {
            "datasets": datasets or [],
            "states": states or {},
            "uf_values": uf_values or {},
            "count_curated": count_curated or [],
            "bank_credit_card_counts_curated": count_curated or [],
            "bank_credit_card_ops_curated": ops_curated or [],
            "upserts": [],
            "ranges": [],
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
        transaction_count_dataset_code=f"{dataset_code}_transaction_count",
        nominal_volume_dataset_code=f"{dataset_code}_nominal_volume",
        transaction_count_source_tag="count-tag",
        nominal_volume_source_tag="volume-tag",
        source_nombre=operation_type,
        source_description=f"{operation_type} description",
        source_endpoint_base="https://cmf.example",
        refresh_frequency="monthly",
        start_date=date(2009, 4, 1),
    )


def _batch(
    dataset_code: str,
    operation_type: str,
    period_month: date,
    rows_synced: int = 1,
    transaction_count_period_month: date | None = None,
    nominal_volume_period_month: date | None = None,
):
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
            nominal_volume_millions_clp=Decimal("120507.338"),
            source_payload={"transaction_count": {}, "nominal_volume_millions_clp": {}},
        )
        for _ in range(rows_synced)
    ]

    return BankCreditCardOpsObservationBatch(
        raw_observations=raw_observations,
        latest_source_month=period_month,
        earliest_source_month=period_month,
        latest_transaction_count_source_month=transaction_count_period_month or period_month,
        latest_nominal_volume_source_month=nominal_volume_period_month or period_month,
    )


def _counts_config() -> BankCreditCardCountsConfig:
    return BankCreditCardCountsConfig(
        dataset_code=BANK_CREDIT_CARD_COUNTS_DATASET,
        active_cards_primary_dataset_code=BANK_CREDIT_CARD_ACTIVE_CARDS_PRIMARY_DATASET,
        active_cards_supplementary_dataset_code=BANK_CREDIT_CARD_ACTIVE_CARDS_SUPPLEMENTARY_DATASET,
        cards_with_operations_primary_dataset_code=BANK_CREDIT_CARD_CARDS_WITH_OPERATIONS_PRIMARY_DATASET,
        cards_with_operations_supplementary_dataset_code=BANK_CREDIT_CARD_CARDS_WITH_OPERATIONS_SUPPLEMENTARY_DATASET,
        active_cards_primary_source_tag="active-primary",
        active_cards_supplementary_source_tag="active-supplementary",
        cards_with_operations_primary_source_tag="ops-primary",
        cards_with_operations_supplementary_source_tag="ops-supplementary",
        source_endpoint_base="https://cmf.example",
        refresh_frequency="monthly",
        start_date=date(2009, 4, 1),
    )


def _counts_batch(period_month: date) -> BankCreditCardCountsObservationBatch:
    return BankCreditCardCountsObservationBatch(
        raw_observations=[
            BankCreditCardCountRawObservation(
                dataset_code=BANK_CREDIT_CARD_ACTIVE_CARDS_PRIMARY_DATASET,
                source_series_id="401",
                source_codigo="SBIF_TCRED_BANC_VIGTIT_AGIFI_BICE_NUM",
                source_nombre="Banco BICE",
                institution_code="BICE",
                institution_name="Banco BICE",
                period_month=period_month,
                card_count=Decimal("100"),
                source_payload={},
            ),
            BankCreditCardCountRawObservation(
                dataset_code=BANK_CREDIT_CARD_ACTIVE_CARDS_SUPPLEMENTARY_DATASET,
                source_series_id="402",
                source_codigo="SBIF_TCRED_BANC_VIGADIC_AGIFI_BICE_NUM",
                source_nombre="Banco BICE",
                institution_code="BICE",
                institution_name="Banco BICE",
                period_month=period_month,
                card_count=Decimal("10"),
                source_payload={},
            ),
            BankCreditCardCountRawObservation(
                dataset_code=BANK_CREDIT_CARD_CARDS_WITH_OPERATIONS_PRIMARY_DATASET,
                source_series_id="403",
                source_codigo="SBIF_TCRED_BANC_COPETIT_AGIFI_BICE_NUM",
                source_nombre="Banco BICE",
                institution_code="BICE",
                institution_name="Banco BICE",
                period_month=period_month,
                card_count=Decimal("80"),
                source_payload={},
            ),
            BankCreditCardCountRawObservation(
                dataset_code=BANK_CREDIT_CARD_CARDS_WITH_OPERATIONS_SUPPLEMENTARY_DATASET,
                source_series_id="404",
                source_codigo="SBIF_TCRED_BANC_COPEADIC_AGIFI_BICE_NUM",
                source_nombre="Banco BICE",
                institution_code="BICE",
                institution_name="Banco BICE",
                period_month=period_month,
                card_count=Decimal("5"),
                source_payload={},
            ),
        ],
        latest_source_month=period_month,
        earliest_source_month=period_month,
        latest_active_cards_primary_source_month=period_month,
        latest_active_cards_supplementary_source_month=period_month,
        latest_cards_with_operations_primary_source_month=period_month,
        latest_cards_with_operations_supplementary_source_month=period_month,
    )


def test_load_active_operation_configs_reads_registry_rows():
    sb = FakeSupabase(
        datasets=[
            {
                "operation_type": "Compras",
                "dataset_code": BANK_CREDIT_CARD_OPS_COMPRAS_TRANSACTION_COUNT_DATASET,
                "measure_kind": CMF_MEASURE_KIND_TRANSACTION_COUNT,
                "source_tag": "count-tag",
                "source_nombre": "Compras",
                "source_description": "Compras desc",
                "source_endpoint_base": "https://cmf.example",
                "refresh_frequency": "monthly",
                "start_date": "2009-04-01",
                "is_active": True,
            },
            {
                "operation_type": "Compras",
                "dataset_code": BANK_CREDIT_CARD_OPS_COMPRAS_NOMINAL_VOLUME_DATASET,
                "measure_kind": CMF_MEASURE_KIND_NOMINAL_VOLUME,
                "source_tag": "volume-tag",
                "source_nombre": "Compras",
                "source_description": "Compras desc",
                "source_endpoint_base": "https://cmf.example",
                "refresh_frequency": "monthly",
                "start_date": "2009-04-01",
                "is_active": True,
            },
            {
                "operation_type": "Avance en Efectivo",
                "dataset_code": BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_TRANSACTION_COUNT_DATASET,
                "measure_kind": CMF_MEASURE_KIND_TRANSACTION_COUNT,
                "source_tag": "count-tag-2",
                "source_nombre": "Avance en Efectivo",
                "source_description": "Avance desc",
                "source_endpoint_base": "https://cmf.example",
                "refresh_frequency": "monthly",
                "start_date": "2009-04-01",
                "is_active": True,
            },
            {
                "operation_type": "Avance en Efectivo",
                "dataset_code": BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_NOMINAL_VOLUME_DATASET,
                "measure_kind": CMF_MEASURE_KIND_NOMINAL_VOLUME,
                "source_tag": "volume-tag-2",
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
        BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_DATASET,
        BANK_CREDIT_CARD_OPS_COMPRAS_DATASET,
    ]


def test_load_active_card_counts_config_reads_registry_rows():
    sb = FakeSupabase(
        datasets=[
            {
                "dataset_code": BANK_CREDIT_CARD_ACTIVE_CARDS_PRIMARY_DATASET,
                "operation_type": "Operations Rate",
                "measure_kind": CMF_MEASURE_KIND_ACTIVE_CARDS_PRIMARY,
                "source_tag": "active-primary",
                "source_endpoint_base": "https://cmf.example",
                "refresh_frequency": "monthly",
                "start_date": "2009-04-01",
                "is_active": True,
            },
            {
                "dataset_code": BANK_CREDIT_CARD_ACTIVE_CARDS_SUPPLEMENTARY_DATASET,
                "operation_type": "Operations Rate",
                "measure_kind": CMF_MEASURE_KIND_ACTIVE_CARDS_SUPPLEMENTARY,
                "source_tag": "active-supplementary",
                "source_endpoint_base": "https://cmf.example",
                "refresh_frequency": "monthly",
                "start_date": "2009-04-01",
                "is_active": True,
            },
            {
                "dataset_code": BANK_CREDIT_CARD_CARDS_WITH_OPERATIONS_PRIMARY_DATASET,
                "operation_type": "Operations Rate",
                "measure_kind": CMF_MEASURE_KIND_CARDS_WITH_OPERATIONS_PRIMARY,
                "source_tag": "ops-primary",
                "source_endpoint_base": "https://cmf.example",
                "refresh_frequency": "monthly",
                "start_date": "2009-04-01",
                "is_active": True,
            },
            {
                "dataset_code": BANK_CREDIT_CARD_CARDS_WITH_OPERATIONS_SUPPLEMENTARY_DATASET,
                "operation_type": "Operations Rate",
                "measure_kind": CMF_MEASURE_KIND_CARDS_WITH_OPERATIONS_SUPPLEMENTARY,
                "source_tag": "ops-supplementary",
                "source_endpoint_base": "https://cmf.example",
                "refresh_frequency": "monthly",
                "start_date": "2009-04-01",
                "is_active": True,
            },
        ]
    )

    config = load_active_card_counts_config(sb)

    assert config is not None
    assert config.dataset_code == BANK_CREDIT_CARD_COUNTS_DATASET
    assert config.active_cards_primary_dataset_code == BANK_CREDIT_CARD_ACTIVE_CARDS_PRIMARY_DATASET


def test_sync_operation_once_noops_when_source_month_is_unchanged(monkeypatch):
    dataset = _config(BANK_CREDIT_CARD_OPS_COMPRAS_DATASET, "Compras")
    sb = FakeSupabase(
        states={
            f"{BANK_CREDIT_CARD_OPS_COMPRAS_DATASET}_transaction_count": [
                {"latest_source_month": "2026-04-01"}
            ],
            f"{BANK_CREDIT_CARD_OPS_COMPRAS_DATASET}_nominal_volume": [
                {"latest_source_month": "2026-04-01"}
            ],
        },
        uf_values={"2026-04-15": [{"value": "40000"}]},
        ops_curated=[{"period_month": "2026-04-01"}],
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
    assert len(sb.upserts) == 2
    assert "last_attempted_sync_at" in sb.upserts[0]["payload"]
    assert "last_attempted_sync_at" in sb.upserts[1]["payload"]


def test_sync_operation_once_syncs_newer_source_and_advances_state(monkeypatch):
    dataset = _config(BANK_CREDIT_CARD_OPS_COMPRAS_DATASET, "Compras")
    sb = FakeSupabase(
        states={
            f"{BANK_CREDIT_CARD_OPS_COMPRAS_DATASET}_transaction_count": [
                {"latest_source_month": "2026-03-01"}
            ],
            f"{BANK_CREDIT_CARD_OPS_COMPRAS_DATASET}_nominal_volume": [
                {"latest_source_month": "2026-03-01"}
            ],
        },
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
    assert sb.upserts[2]["table"] == "bank_credit_card_ops_raw"
    assert sb.upserts[2]["payload"][0]["transaction_count"] == "2500"
    assert sb.upserts[3]["table"] == "bank_credit_card_ops_curated"
    assert sb.upserts[3]["payload"][0]["average_ticket_uf"] == "1205.07338000000"
    assert sb.upserts[3]["payload"][0]["nominal_volume_millions_clp"] == "120507.338"
    assert sb.upserts[4]["table"] == "cmf_dataset_sync_state"
    assert sb.upserts[4]["payload"]["latest_source_month"] == "2026-04-01"
    assert sb.upserts[4]["payload"]["latest_curated_month"] == "2026-04-01"
    assert sb.upserts[4]["payload"]["last_error"] is None
    assert sb.upserts[5]["table"] == "cmf_dataset_sync_state"
    assert sb.upserts[5]["payload"]["latest_source_month"] == "2026-04-01"
    assert sb.upserts[5]["payload"]["latest_curated_month"] == "2026-04-01"
    assert sb.upserts[5]["payload"]["last_error"] is None


def test_sync_operation_once_syncs_when_only_one_endpoint_advances(monkeypatch):
    dataset = _config(BANK_CREDIT_CARD_OPS_COMPRAS_DATASET, "Compras")
    sb = FakeSupabase(
        states={
            f"{BANK_CREDIT_CARD_OPS_COMPRAS_DATASET}_transaction_count": [
                {"latest_source_month": "2026-04-01"}
            ],
            f"{BANK_CREDIT_CARD_OPS_COMPRAS_DATASET}_nominal_volume": [
                {"latest_source_month": "2026-03-01"}
            ],
        },
        uf_values={"2026-04-15": [{"value": "40000"}]},
    )

    async def fake_fetch_operation_batch(_client, *, config, fecha_fin):
        return _batch(
            config.dataset_code,
            config.operation_type,
            date(2026, 4, 1),
            transaction_count_period_month=date(2026, 4, 1),
            nominal_volume_period_month=date(2026, 4, 1),
        )

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
    assert sb.upserts[4]["payload"]["dataset_code"] == f"{BANK_CREDIT_CARD_OPS_COMPRAS_DATASET}_transaction_count"
    assert sb.upserts[5]["payload"]["dataset_code"] == f"{BANK_CREDIT_CARD_OPS_COMPRAS_DATASET}_nominal_volume"


def test_sync_operation_once_records_failure_without_advancing_state(monkeypatch):
    dataset = _config(BANK_CREDIT_CARD_OPS_COMPRAS_DATASET, "Compras")
    sb = FakeSupabase(
        states={
            f"{BANK_CREDIT_CARD_OPS_COMPRAS_DATASET}_transaction_count": [
                {"latest_source_month": "2026-03-01"}
            ],
            f"{BANK_CREDIT_CARD_OPS_COMPRAS_DATASET}_nominal_volume": [
                {"latest_source_month": "2026-03-01"}
            ],
        },
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

    assert sb.upserts[-2]["payload"]["last_error"] == "RuntimeError: load failed"
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


def test_sync_card_counts_once_syncs_newer_source_and_advances_state(monkeypatch):
    config = _counts_config()
    sb = FakeSupabase(
        states={
            BANK_CREDIT_CARD_ACTIVE_CARDS_PRIMARY_DATASET: [
                {"latest_source_month": "2026-03-01"}
            ],
            BANK_CREDIT_CARD_ACTIVE_CARDS_SUPPLEMENTARY_DATASET: [
                {"latest_source_month": "2026-03-01"}
            ],
            BANK_CREDIT_CARD_CARDS_WITH_OPERATIONS_PRIMARY_DATASET: [
                {"latest_source_month": "2026-03-01"}
            ],
            BANK_CREDIT_CARD_CARDS_WITH_OPERATIONS_SUPPLEMENTARY_DATASET: [
                {"latest_source_month": "2026-03-01"}
            ],
        },
    )

    async def fake_fetch_card_counts_batch(_client, *, config, fecha_fin):
        return _counts_batch(date(2026, 4, 1))

    monkeypatch.setattr(
        "data.workers.bank_credit_card_ops_worker.fetch_card_counts_batch",
        fake_fetch_card_counts_batch,
    )

    rows_synced = asyncio.run(
        sync_card_counts_once(
            None,
            sb,
            config=config,
            run_date=date(2026, 4, 24),
        )
    )

    assert rows_synced == 4
    assert sb.upserts[4]["table"] == "bank_credit_card_counts_raw"
    assert sb.upserts[5]["table"] == "bank_credit_card_counts_curated"
    assert sb.upserts[5]["payload"][0]["total_active_cards"] == "110"
    assert sb.upserts[-1]["payload"]["latest_curated_month"] == "2026-04-01"


def test_sync_all_bank_credit_card_ops_once_includes_card_counts(monkeypatch):
    configs = [_config(BANK_CREDIT_CARD_OPS_COMPRAS_DATASET, "Compras")]
    sb = FakeSupabase()
    count_config = _counts_config()

    async def fake_sync_operation_once(_client, _sb, *, config, run_date):
        return 3

    async def fake_sync_card_counts_once(_client, _sb, *, config, run_date):
        return 4

    monkeypatch.setattr(
        "data.workers.bank_credit_card_ops_worker.sync_operation_once",
        fake_sync_operation_once,
    )
    monkeypatch.setattr(
        "data.workers.bank_credit_card_ops_worker.sync_card_counts_once",
        fake_sync_card_counts_once,
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
            card_counts=count_config,
        )
    )

    assert results == {
        BANK_CREDIT_CARD_COUNTS_DATASET: 4,
        BANK_CREDIT_CARD_OPS_COMPRAS_DATASET: 3,
    }


def test_build_active_cards_lookup_paginates_for_large_tables():
    count_curated = []
    for i in range(1205):
        year = 2000 + (i // 12)
        month = 1 + (i % 12)
        count_curated.append(
            {
                "dataset_code": BANK_CREDIT_CARD_COUNTS_DATASET,
                "institution_code": "TEST",
                "period_month": date(year, month, 1).isoformat(),
                "total_active_cards": i + 1,
            }
        )

    sb = FakeSupabase(count_curated=count_curated)
    lookup = build_active_cards_lookup(sb)

    assert lookup("TEST", date(2083, 5, 1)) == Decimal("1001")
    assert any(
        call["table"] == "bank_credit_card_counts_curated" and call["start"] == 1000
        for call in sb.db["ranges"]
    )

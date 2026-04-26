import asyncio
from datetime import date

import pytest

from data.workers.cmf_monthly_worker import (
    CmfMonthlyDataset,
    CmfMonthlyWorkerConfig,
    active_monthly_datasets,
    latest_observation_month,
    sync_all_cmf_monthly_datasets_once,
    sync_cmf_monthly_dataset_once,
)


class Observation:
    def __init__(self, period_month):
        self.period_month = period_month


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

        return FakeResponse(self.db["states"].get(self._eq_filter[1], []))


class FakeSupabase:
    def __init__(self, states=None):
        self.db = {
            "states": states or {},
            "upserts": [],
        }

    def table(self, name):
        return FakeTable(name, self.db)

    @property
    def upserts(self):
        return self.db["upserts"]


def _dataset(dataset_code, latest_source_month, rows_synced=1):
    async def fetch_latest_source_month(_client, _endpoint_base, _run_date):
        return latest_source_month

    async def sync_dataset(_client, _sb, _endpoint_base, _run_date):
        return rows_synced

    return CmfMonthlyDataset(
        dataset_code=dataset_code,
        fetch_latest_source_month=fetch_latest_source_month,
        sync_dataset=sync_dataset,
    )


def _failing_dataset(dataset_code, error_message):
    async def fetch_latest_source_month(_client, _endpoint_base, _run_date):
        raise RuntimeError(error_message)

    async def sync_dataset(_client, _sb, _endpoint_base, _run_date):
        return 0

    return CmfMonthlyDataset(
        dataset_code=dataset_code,
        fetch_latest_source_month=fetch_latest_source_month,
        sync_dataset=sync_dataset,
    )


def test_latest_observation_month_returns_max_month():
    assert latest_observation_month(
        [
            Observation(date(2026, 3, 1)),
            Observation(date(2026, 4, 1)),
        ]
    ) == date(2026, 4, 1)


def test_sync_cmf_monthly_dataset_once_noops_when_source_month_is_unchanged():
    dataset = _dataset("dataset", date(2026, 4, 1))
    sb = FakeSupabase(states={"dataset": [{"latest_source_month": "2026-04-01"}]})

    rows_synced = asyncio.run(
        sync_cmf_monthly_dataset_once(
            None,
            sb,
            dataset=dataset,
            endpoint_base="https://cmf.example",
            run_date=date(2026, 4, 24),
        )
    )

    assert rows_synced == 0
    assert len(sb.upserts) == 1
    assert "last_attempted_sync_at" in sb.upserts[0]["payload"]


def test_sync_cmf_monthly_dataset_once_syncs_newer_source_and_advances_state():
    dataset = _dataset("dataset", date(2026, 4, 1), rows_synced=5)
    sb = FakeSupabase(states={"dataset": [{"latest_source_month": "2026-03-01"}]})

    rows_synced = asyncio.run(
        sync_cmf_monthly_dataset_once(
            None,
            sb,
            dataset=dataset,
            endpoint_base="https://cmf.example",
            run_date=date(2026, 4, 24),
        )
    )

    assert rows_synced == 5
    assert sb.upserts[-1]["payload"]["latest_source_month"] == "2026-04-01"
    assert sb.upserts[-1]["payload"]["latest_curated_month"] == "2026-04-01"
    assert sb.upserts[-1]["payload"]["last_error"] is None


def test_sync_cmf_monthly_dataset_once_records_failure_without_advancing_state():
    async def fetch_latest_source_month(_client, _endpoint_base, _run_date):
        return date(2026, 4, 1)

    async def sync_dataset(_client, _sb, _endpoint_base, _run_date):
        raise RuntimeError("load failed")

    dataset = CmfMonthlyDataset(
        dataset_code="dataset",
        fetch_latest_source_month=fetch_latest_source_month,
        sync_dataset=sync_dataset,
    )
    sb = FakeSupabase(states={"dataset": [{"latest_source_month": "2026-03-01"}]})

    with pytest.raises(RuntimeError, match="load failed"):
        asyncio.run(
            sync_cmf_monthly_dataset_once(
                None,
                sb,
                dataset=dataset,
                endpoint_base="https://cmf.example",
                run_date=date(2026, 4, 24),
            )
        )

    assert sb.upserts[-1]["payload"]["last_error"] == "RuntimeError: load failed"
    assert "latest_source_month" not in sb.upserts[-1]["payload"]
    assert "latest_curated_month" not in sb.upserts[-1]["payload"]


def test_sync_all_cmf_monthly_datasets_once_continues_after_one_dataset_failure():
    datasets = [
        _failing_dataset("transactions", "source unavailable"),
        _dataset("volume", date(2026, 4, 1), rows_synced=3),
    ]
    sb = FakeSupabase(states={"volume": [{"latest_source_month": "2026-03-01"}]})

    results = asyncio.run(
        sync_all_cmf_monthly_datasets_once(
            None,
            sb,
            config=CmfMonthlyWorkerConfig(
                supabase_url="https://supabase.example",
                supabase_service_role_key="service-role",
                endpoint_base="https://cmf.example",
            ),
            run_date=date(2026, 4, 24),
            datasets=datasets,
        )
    )

    assert results == {"transactions": 0, "volume": 3}
    assert sb.upserts[1]["payload"]["last_error"] == "RuntimeError: source unavailable"


def test_sync_all_cmf_monthly_datasets_once_runs_both_datasets():
    datasets = [
        _dataset("transactions", date(2026, 4, 1), rows_synced=2),
        _dataset("volume", date(2026, 4, 1), rows_synced=3),
    ]
    sb = FakeSupabase()

    results = asyncio.run(
        sync_all_cmf_monthly_datasets_once(
            None,
            sb,
            config=CmfMonthlyWorkerConfig(
                supabase_url="https://supabase.example",
                supabase_service_role_key="service-role",
                endpoint_base="https://cmf.example",
            ),
            run_date=date(2026, 4, 24),
            datasets=datasets,
        )
    )

    assert results == {"transactions": 2, "volume": 3}


def test_active_monthly_datasets_includes_transaction_and_purchase_volume():
    dataset_codes = {dataset.dataset_code for dataset in active_monthly_datasets()}

    assert dataset_codes == {
        "bank_credit_card_transaction_count",
        "bank_credit_card_purchase_volume",
    }

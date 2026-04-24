import asyncio
from datetime import date

import pytest

import data.workers.uf_worker as uf_worker
from data.models.uf import UfValue
from data.workers.uf_worker import UfWorkerConfig, sync_uf_once


class FakeResponse:
    def __init__(self, data=None):
        self.data = data or []


class FakeTable:
    def __init__(self, name, db):
        self.name = name
        self.db = db
        self._upsert_payload = None

    def select(self, *_args):
        return self

    def order(self, *_args, **_kwargs):
        return self

    def limit(self, *_args):
        return self

    def upsert(self, payload, **kwargs):
        self._upsert_payload = payload
        self.db["upserts"].append(
            {
                "table": self.name,
                "payload": payload,
                "kwargs": kwargs,
            }
        )
        return self

    def execute(self):
        if self.name == "uf_values" and self._upsert_payload is None:
            latest = self.db["latest_stored_date"]
            return FakeResponse([{"uf_date": latest.isoformat()}] if latest else [])

        return FakeResponse()


class FakeSupabase:
    def __init__(self, latest_stored_date=None):
        self.db = {
            "latest_stored_date": latest_stored_date,
            "upserts": [],
        }

    def table(self, name):
        return FakeTable(name, self.db)

    @property
    def upserts(self):
        return self.db["upserts"]


@pytest.fixture
def config():
    return UfWorkerConfig(
        cmf_api_key="secret",
        base_endpoint_cmf_uf="https://cmf.example",
        supabase_url="https://supabase.example",
        supabase_service_role_key="service-role",
    )


def test_sync_uf_once_noops_when_source_date_is_unchanged(monkeypatch, config):
    async def fake_fetch_historical_ufs(*_args, **_kwargs):
        return [UfValue(date(2026, 4, 15), 39200.0)]

    monkeypatch.setattr(uf_worker, "fetch_historical_ufs", fake_fetch_historical_ufs)
    sb = FakeSupabase(latest_stored_date=date(2026, 4, 15))

    rows_synced = asyncio.run(sync_uf_once(None, sb, config))

    assert rows_synced == 0
    assert [upsert["table"] for upsert in sb.upserts] == ["uf_sync_runs"]
    assert sb.upserts[0]["payload"]["latest_source_uf_date"] == "2026-04-15"
    assert sb.upserts[0]["payload"]["rows_upserted"] == 0
    assert "synced_at" in sb.upserts[0]["payload"]


def test_sync_uf_once_upserts_only_new_rows_and_updates_state(monkeypatch, config):
    async def fake_fetch_historical_ufs(*_args, **_kwargs):
        return [
            UfValue(date(2026, 4, 14), 39100.0),
            UfValue(date(2026, 4, 15), 39200.0),
            UfValue(date(2026, 4, 16), 39300.0),
        ]

    monkeypatch.setattr(uf_worker, "fetch_historical_ufs", fake_fetch_historical_ufs)
    sb = FakeSupabase(latest_stored_date=date(2026, 4, 15))

    rows_synced = asyncio.run(sync_uf_once(None, sb, config))

    assert rows_synced == 1
    assert [upsert["table"] for upsert in sb.upserts] == ["uf_values", "uf_sync_runs"]
    assert sb.upserts[0]["payload"] == [{"uf_date": "2026-04-16", "value": 39300.0}]
    assert sb.upserts[1]["payload"]["latest_source_uf_date"] == "2026-04-16"
    assert sb.upserts[1]["payload"]["latest_stored_uf_date"] == "2026-04-16"
    assert sb.upserts[1]["payload"]["rows_upserted"] == 1
    assert "synced_at" in sb.upserts[1]["payload"]

from datetime import date

from data.loaders.uf_loader import (
    latest_stored_uf_date,
    new_uf_values,
    record_uf_sync_failure,
)
from data.models.uf import UfValue


class FakeResponse:
    def __init__(self, data):
        self.data = data


class FakeQuery:
    def __init__(self, response_data):
        self.response_data = response_data

    def select(self, *_args):
        return self

    def order(self, *_args, **_kwargs):
        return self

    def limit(self, *_args):
        return self

    def execute(self):
        return FakeResponse(self.response_data)


class FakeSupabase:
    def __init__(self, response_data):
        self.response_data = response_data
        self.upserts = []

    def table(self, table_name):
        assert table_name == "uf_values"
        return FakeQuery(self.response_data)


class FakeSyncTable:
    def __init__(self, db):
        self.db = db
        self._payload = None

    def upsert(self, payload, **kwargs):
        self._payload = payload
        self.db["upserts"].append({"payload": payload, "kwargs": kwargs})
        return self

    def execute(self):
        return None


class FakeSyncSupabase:
    def __init__(self):
        self.db = {"upserts": []}

    def table(self, table_name):
        assert table_name == "uf_sync_runs"
        return FakeSyncTable(self.db)

    @property
    def upserts(self):
        return self.db["upserts"]


def test_latest_stored_uf_date_returns_none_when_table_is_empty():
    assert latest_stored_uf_date(FakeSupabase([])) is None


def test_latest_stored_uf_date_reads_latest_row():
    sb = FakeSupabase([{"uf_date": "2026-04-15"}])

    assert latest_stored_uf_date(sb) == date(2026, 4, 15)


def test_new_uf_values_returns_only_rows_after_latest_stored_date():
    source_values = [
        UfValue(date(2026, 4, 14), 39100.0),
        UfValue(date(2026, 4, 15), 39200.0),
        UfValue(date(2026, 4, 16), 39300.0),
    ]

    assert new_uf_values(source_values, date(2026, 4, 15)) == [
        UfValue(date(2026, 4, 16), 39300.0)
    ]


def test_new_uf_values_returns_all_rows_when_no_stored_date_exists():
    source_values = [
        UfValue(date(2026, 4, 14), 39100.0),
        UfValue(date(2026, 4, 15), 39200.0),
    ]

    assert new_uf_values(source_values, None) == source_values


def test_record_uf_sync_failure_writes_last_error_state():
    sb = FakeSyncSupabase()

    record_uf_sync_failure(sb, RuntimeError("source failed"))

    assert sb.upserts[0]["payload"]["sync_key"] == "uf_values"
    assert sb.upserts[0]["payload"]["last_error"] == "RuntimeError: source failed"
    assert "synced_at" in sb.upserts[0]["payload"]

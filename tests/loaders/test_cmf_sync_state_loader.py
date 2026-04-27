from datetime import date
from datetime import datetime
from zoneinfo import ZoneInfo

from data.loaders.bank_credit_card_ops_sync_state_loader import (
    get_latest_state_source_month,
    record_sync_attempt,
    record_sync_failure,
    record_sync_success,
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


def test_get_latest_state_source_month_returns_none_when_missing():
    assert get_latest_state_source_month(FakeSupabase(), "dataset") is None


def test_get_latest_state_source_month_reads_existing_state():
    sb = FakeSupabase(
        states={"dataset": [{"latest_source_month": "2026-04-01"}]},
    )

    assert get_latest_state_source_month(sb, "dataset") == date(2026, 4, 1)


def test_record_sync_attempt_writes_attempt_timestamp(monkeypatch):
    sb = FakeSupabase()

    fixed_now = datetime(2026, 4, 24, 12, 34, 56, tzinfo=ZoneInfo("America/Santiago"))
    import data.loaders.bank_credit_card_ops_sync_state_loader as sync_state_loader

    monkeypatch.setattr(sync_state_loader, "now_santiago", lambda: fixed_now)
    record_sync_attempt(sb, "dataset")

    assert sb.upserts[0]["table"] == "bank_credit_card_ops_sync_state"
    assert sb.upserts[0]["payload"]["dataset_code"] == "dataset"
    assert sb.upserts[0]["payload"]["last_attempted_sync_at"] == fixed_now.isoformat()


def test_record_sync_success_advances_source_and_curated_months(monkeypatch):
    sb = FakeSupabase()

    fixed_now = datetime(2026, 4, 24, 12, 34, 56, tzinfo=ZoneInfo("America/Santiago"))
    import data.loaders.bank_credit_card_ops_sync_state_loader as sync_state_loader

    monkeypatch.setattr(sync_state_loader, "now_santiago", lambda: fixed_now)
    record_sync_success(
        sb,
        dataset_code="dataset",
        latest_source_month=date(2026, 4, 1),
        latest_curated_month=date(2026, 4, 1),
    )

    assert sb.upserts[0]["payload"]["latest_source_month"] == "2026-04-01"
    assert sb.upserts[0]["payload"]["latest_curated_month"] == "2026-04-01"
    assert sb.upserts[0]["payload"]["last_error"] is None
    assert sb.upserts[0]["payload"]["last_successful_sync_at"] == fixed_now.isoformat()


def test_record_sync_failure_records_error_without_advancing_months(monkeypatch):
    sb = FakeSupabase()

    fixed_now = datetime(2026, 4, 24, 12, 34, 56, tzinfo=ZoneInfo("America/Santiago"))
    import data.loaders.bank_credit_card_ops_sync_state_loader as sync_state_loader

    monkeypatch.setattr(sync_state_loader, "now_santiago", lambda: fixed_now)
    record_sync_failure(
        sb,
        dataset_code="dataset",
        error=ValueError("boom"),
    )

    assert sb.upserts[0]["payload"]["dataset_code"] == "dataset"
    assert sb.upserts[0]["payload"]["last_error"] == "ValueError: boom"
    assert "latest_source_month" not in sb.upserts[0]["payload"]
    assert "latest_curated_month" not in sb.upserts[0]["payload"]
    assert sb.upserts[0]["payload"]["updated_at"] == fixed_now.isoformat()

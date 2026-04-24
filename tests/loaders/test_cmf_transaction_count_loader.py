from datetime import date
from decimal import Decimal

from data.loaders.cmf_transaction_count_loader import (
    latest_curated_transaction_count_month,
    upsert_transaction_count_curated,
    upsert_transaction_count_raw,
)
from data.models.cmf_cards import (
    BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET,
    CmfTransactionCountCuratedObservation,
    CmfTransactionCountRawObservation,
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


def _raw_observation():
    return CmfTransactionCountRawObservation(
        dataset_code=BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET,
        source_series_id="101",
        source_codigo="SBIF_TCRED_BANC_COMP_AGIFI_001_NUM",
        source_nombre="Banco Uno",
        institution_code="001",
        institution_name="Banco Uno",
        period_month=date(2026, 4, 1),
        transaction_count=Decimal("1234"),
        source_payload={"Fecha": "2026-04-01", "Valor": "1.234"},
    )


def _curated_observation():
    return CmfTransactionCountCuratedObservation(
        institution_code="001",
        institution_name="Banco Uno",
        period_month=date(2026, 4, 1),
        transaction_count=Decimal("1234"),
        source_dataset_code=BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET,
    )


def test_latest_curated_transaction_count_month_returns_none_for_empty_table():
    assert latest_curated_transaction_count_month(FakeSupabase()) is None


def test_latest_curated_transaction_count_month_reads_latest_row():
    sb = FakeSupabase(latest_curated=[{"period_month": "2026-04-01"}])

    assert latest_curated_transaction_count_month(sb) == date(2026, 4, 1)


def test_upsert_transaction_count_raw_uses_idempotent_conflict_key():
    sb = FakeSupabase()

    upsert_transaction_count_raw(sb, [_raw_observation()])

    assert sb.upserts[0]["table"] == "cmf_card_transaction_count_raw"
    assert sb.upserts[0]["kwargs"] == {
        "on_conflict": "dataset_code,source_codigo,period_month"
    }
    assert sb.upserts[0]["payload"][0]["transaction_count"] == "1234"


def test_upsert_transaction_count_curated_uses_idempotent_conflict_key():
    sb = FakeSupabase()

    upsert_transaction_count_curated(sb, [_curated_observation()])

    assert sb.upserts[0]["table"] == "cmf_card_transaction_count_curated"
    assert sb.upserts[0]["kwargs"] == {
        "on_conflict": "institution_code,period_month"
    }
    assert sb.upserts[0]["payload"][0]["source_dataset_code"] == (
        BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET
    )

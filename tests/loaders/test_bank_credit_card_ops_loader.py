from datetime import date
from decimal import Decimal

import pytest

from data.loaders.bank_credit_card_ops_loader import (
    get_uf_value_for_date,
    latest_curated_operation_month,
    upsert_bank_credit_card_ops_curated,
    upsert_bank_credit_card_ops_raw,
)
from data.models.bank_credit_card_operations import (
    BANK_CREDIT_CARD_OPS_COMPRAS_DATASET,
    BANK_CREDIT_CARD_OPS_CURATED_TABLE,
    BANK_CREDIT_CARD_OPS_RAW_TABLE,
    BankCreditCardOpsCuratedObservation,
    BankCreditCardOpsRawObservation,
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

        if self.name == BANK_CREDIT_CARD_OPS_CURATED_TABLE:
            return FakeResponse(
                self.db["curated"] or self.db["latest_curated"]
            )

        return FakeResponse(self.db["latest_curated"])


class FakeSupabase:
    def __init__(self, latest_curated=None, uf_values=None, curated=None):
        self.db = {
            "latest_curated": latest_curated or [],
            "uf_values": uf_values or {},
            "curated": curated or [],
            "upserts": [],
        }

    def table(self, name):
        return FakeTable(name, self.db)

    @property
    def upserts(self):
        return self.db["upserts"]


def _raw_observation():
    return BankCreditCardOpsRawObservation(
        operation_type="Compras",
        dataset_code=BANK_CREDIT_CARD_OPS_COMPRAS_DATASET,
        source_series_id="301",
        source_codigo="SBIF_TCRED_BANC_COMP_AGIFI_BICE_$",
        source_nombre="Banco BICE",
        institution_code="BICE",
        institution_name="Banco BICE",
        period_month=date(2026, 4, 1),
        transaction_count=Decimal("2500"),
        nominal_volume_millions_clp=Decimal("120507338"),
        source_payload={
            "transaction_count": {"Fecha": "2026-04-01", "Valor": "2.500"},
            "nominal_volume_millions_clp": {
                "Fecha": "2026-04-01",
                "Valor": "120.507.338",
            },
        },
    )


def _curated_observation():
    return BankCreditCardOpsCuratedObservation(
        operation_type="Compras",
        dataset_code=BANK_CREDIT_CARD_OPS_COMPRAS_DATASET,
        institution_code="BICE",
        institution_name="Banco BICE",
        period_month=date(2026, 4, 1),
        transaction_count=Decimal("2500"),
        nominal_volume_thousands_millions_clp=Decimal("120507.338"),
        uf_date_used=date(2026, 4, 15),
        uf_value_used=Decimal("40000"),
        real_value_uf=Decimal("3.01268345"),
        average_ticket_uf=Decimal("1205073.38"),
        source_dataset_code=BANK_CREDIT_CARD_OPS_COMPRAS_DATASET,
    )


def test_latest_curated_operation_month_returns_none_for_empty_table():
    assert latest_curated_operation_month(FakeSupabase(), dataset_code=BANK_CREDIT_CARD_OPS_COMPRAS_DATASET) is None


def test_latest_curated_operation_month_reads_latest_row():
    sb = FakeSupabase(latest_curated=[{"period_month": "2026-04-01"}])

    assert (
        latest_curated_operation_month(sb, dataset_code=BANK_CREDIT_CARD_OPS_COMPRAS_DATASET)
        == date(2026, 4, 1)
    )


def test_get_uf_value_for_date_returns_decimal_value():
    sb = FakeSupabase(uf_values={"2026-04-15": [{"value": "40000.25"}]})

    assert get_uf_value_for_date(sb, date(2026, 4, 15)) == Decimal("40000.25")


def test_get_uf_value_for_date_raises_when_missing():
    with pytest.raises(ValueError, match="Missing UF value for 2026-04-15"):
        get_uf_value_for_date(FakeSupabase(), date(2026, 4, 15))


def test_upsert_bank_credit_card_ops_raw_uses_idempotent_conflict_key():
    sb = FakeSupabase()

    upsert_bank_credit_card_ops_raw(sb, [_raw_observation()])

    assert sb.upserts[0]["table"] == BANK_CREDIT_CARD_OPS_RAW_TABLE
    assert sb.upserts[0]["kwargs"] == {
        "on_conflict": "dataset_code,source_codigo,period_month"
    }
    assert sb.upserts[0]["payload"][0]["nominal_volume_millions_clp"] == "120507338"


def test_upsert_bank_credit_card_ops_curated_uses_idempotent_conflict_key():
    sb = FakeSupabase()

    upsert_bank_credit_card_ops_curated(sb, [_curated_observation()])

    assert sb.upserts[0]["table"] == BANK_CREDIT_CARD_OPS_CURATED_TABLE
    assert sb.upserts[0]["kwargs"] == {
        "on_conflict": "dataset_code,institution_code,period_month"
    }
    assert sb.upserts[0]["payload"][0]["average_ticket_uf"] == "1205073.38"

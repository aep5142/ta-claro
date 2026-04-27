from datetime import date
from decimal import Decimal

from data.models.bank_credit_card_operations import (
    BANK_CREDIT_CARD_OPS_CURATED_TABLE,
    BANK_CREDIT_CARD_OPS_RAW_TABLE,
    BankCreditCardOpsCuratedObservation,
    BankCreditCardOpsRawObservation,
)


def latest_curated_operation_month(sb, *, dataset_code: str) -> date | None:
    response = (
        sb.table(BANK_CREDIT_CARD_OPS_CURATED_TABLE)
        .select("period_month")
        .eq("dataset_code", dataset_code)
        .order("period_month", desc=True)
        .limit(1)
        .execute()
    )

    if not response.data:
        return None

    return date.fromisoformat(response.data[0]["period_month"])


def get_uf_value_for_date(sb, uf_date: date) -> Decimal:
    response = (
        sb.table("uf_values")
        .select("value")
        .eq("uf_date", uf_date.isoformat())
        .limit(1)
        .execute()
    )

    if not response.data:
        raise ValueError(f"Missing UF value for {uf_date.isoformat()}")

    return Decimal(str(response.data[0]["value"]))


def upsert_bank_credit_card_ops_raw(
    sb,
    observations: list[BankCreditCardOpsRawObservation],
):
    if not observations:
        return None

    return (
        sb.table(BANK_CREDIT_CARD_OPS_RAW_TABLE)
        .upsert(
            [observation.to_row() for observation in observations],
            on_conflict="dataset_code,source_codigo,period_month",
        )
        .execute()
    )


def upsert_bank_credit_card_ops_curated(
    sb,
    observations: list[BankCreditCardOpsCuratedObservation],
):
    if not observations:
        return None

    return (
        sb.table(BANK_CREDIT_CARD_OPS_CURATED_TABLE)
        .upsert(
            [observation.to_row() for observation in observations],
            on_conflict="dataset_code,institution_code,period_month",
        )
        .execute()
    )

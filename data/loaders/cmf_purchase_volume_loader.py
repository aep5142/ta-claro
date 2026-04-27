from datetime import date
from decimal import Decimal

from data.models.bank_credit_card_operations import (
    BANK_CREDIT_CARD_PURCHASE_VOLUME_CURATED_TABLE,
    BANK_CREDIT_CARD_PURCHASE_VOLUME_RAW_TABLE,
    BANK_CREDIT_CARD_PURCHASES_METRICS_TABLE,
    BANK_CREDIT_CARD_TRANSACTION_COUNT_CURATED_TABLE,
    CmfPurchaseVolumeCuratedObservation,
    CmfPurchaseVolumeRawObservation,
    CmfTransactionCountCuratedObservation,
)
from data.transforms.bank_credit_card_purchases_metrics import to_purchases_metrics


def latest_curated_purchase_volume_month(sb) -> date | None:
    response = (
        sb.table(BANK_CREDIT_CARD_PURCHASE_VOLUME_CURATED_TABLE)
        .select("period_month")
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


def upsert_purchase_volume_raw(
    sb,
    observations: list[CmfPurchaseVolumeRawObservation],
):
    if not observations:
        return None

    return (
        sb.table(BANK_CREDIT_CARD_PURCHASE_VOLUME_RAW_TABLE)
        .upsert(
            [observation.to_row() for observation in observations],
            on_conflict="dataset_code,source_codigo,period_month",
        )
        .execute()
    )


def upsert_purchase_volume_curated(
    sb,
    observations: list[CmfPurchaseVolumeCuratedObservation],
):
    if not observations:
        return None

    return (
        sb.table(BANK_CREDIT_CARD_PURCHASE_VOLUME_CURATED_TABLE)
        .upsert(
            [observation.to_row() for observation in observations],
            on_conflict="institution_code,period_month",
        )
        .execute()
    )


def _parse_purchase_volume_rows(rows: list[dict]) -> list[CmfPurchaseVolumeCuratedObservation]:
    return [
        CmfPurchaseVolumeCuratedObservation(
            institution_code=row["institution_code"],
            institution_name=row["institution_name"],
            period_month=date.fromisoformat(row["period_month"]),
            nominal_volume_thousands_millions_clp=Decimal(
                str(row["nominal_volume_thousands_millions_clp"])
            ),
            uf_date_used=date.fromisoformat(row["uf_date_used"]),
            uf_value_used=Decimal(str(row["uf_value_used"])),
            real_volume_uf=Decimal(str(row["real_volume_uf"])),
            source_dataset_code=row["source_dataset_code"],
        )
        for row in rows
    ]


def _parse_transaction_count_rows(rows: list[dict]) -> list[CmfTransactionCountCuratedObservation]:
    return [
        CmfTransactionCountCuratedObservation(
            institution_code=row["institution_code"],
            institution_name=row["institution_name"],
            period_month=date.fromisoformat(row["period_month"]),
            transaction_count=Decimal(str(row["transaction_count"])),
            source_dataset_code=row["source_dataset_code"],
        )
        for row in rows
    ]


def refresh_bank_credit_card_purchases_metrics(sb):
    purchase_volume_response = (
        sb.table(BANK_CREDIT_CARD_PURCHASE_VOLUME_CURATED_TABLE)
        .select(
            "institution_code,institution_name,period_month,nominal_volume_thousands_millions_clp,uf_date_used,uf_value_used,real_volume_uf,source_dataset_code"
        )
        .execute()
    )
    transaction_count_response = (
        sb.table(BANK_CREDIT_CARD_TRANSACTION_COUNT_CURATED_TABLE)
        .select(
            "institution_code,institution_name,period_month,transaction_count,source_dataset_code"
        )
        .execute()
    )
    metrics = to_purchases_metrics(
        _parse_purchase_volume_rows(purchase_volume_response.data or []),
        _parse_transaction_count_rows(transaction_count_response.data or []),
    )

    if not metrics:
        return None

    return (
        sb.table(BANK_CREDIT_CARD_PURCHASES_METRICS_TABLE)
        .upsert(
            [observation.to_row() for observation in metrics],
            on_conflict="institution_code,period_month",
        )
        .execute()
    )

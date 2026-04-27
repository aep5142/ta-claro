from decimal import Decimal

from data.models.bank_credit_card_operations import (
    CmfPurchaseMetricsObservation,
    CmfPurchaseVolumeCuratedObservation,
    CmfTransactionCountCuratedObservation,
)


def to_purchases_metrics(
    purchase_volume_rows: list[CmfPurchaseVolumeCuratedObservation],
    transaction_count_rows: list[CmfTransactionCountCuratedObservation],
) -> list[CmfPurchaseMetricsObservation]:
    transaction_counts_by_key = {
        (row.institution_code, row.period_month): row for row in transaction_count_rows
    }
    metrics: list[CmfPurchaseMetricsObservation] = []

    for purchase_volume_row in purchase_volume_rows:
        transaction_count_row = transaction_counts_by_key.get(
            (purchase_volume_row.institution_code, purchase_volume_row.period_month)
        )
        if transaction_count_row is None:
            continue

        average_ticket_uf = purchase_volume_row.real_volume_uf / transaction_count_row.transaction_count
        metrics.append(
            CmfPurchaseMetricsObservation(
                institution_code=purchase_volume_row.institution_code,
                institution_name=purchase_volume_row.institution_name,
                period_month=purchase_volume_row.period_month,
                nominal_volume_thousands_millions_clp=(
                    purchase_volume_row.nominal_volume_thousands_millions_clp
                ),
                uf_date_used=purchase_volume_row.uf_date_used,
                uf_value_used=purchase_volume_row.uf_value_used,
                real_volume_uf=purchase_volume_row.real_volume_uf,
                transaction_count=transaction_count_row.transaction_count,
                average_ticket_uf=average_ticket_uf,
                source_purchase_volume_dataset_code=purchase_volume_row.source_dataset_code,
                source_transaction_count_dataset_code=transaction_count_row.source_dataset_code,
            )
        )

    return sorted(
        metrics,
        key=lambda observation: (observation.institution_code, observation.period_month),
    )

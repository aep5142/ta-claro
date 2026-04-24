from data.models.cmf_cards import (
    BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET,
    CmfTransactionCountCuratedObservation,
    CmfTransactionCountRawObservation,
)


def to_curated_transaction_count(
    raw_observations: list[CmfTransactionCountRawObservation],
) -> list[CmfTransactionCountCuratedObservation]:
    return [
        CmfTransactionCountCuratedObservation(
            institution_code=observation.institution_code,
            institution_name=observation.institution_name,
            period_month=observation.period_month,
            transaction_count=observation.transaction_count,
            source_dataset_code=BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET,
        )
        for observation in raw_observations
    ]

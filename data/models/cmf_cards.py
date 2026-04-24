from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any


BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET = "bank_credit_card_transaction_count"
BANK_CREDIT_CARD_TRANSACTION_COUNT_TAG = "SBIF_TCRED_BANC_COMP_AGIFI_NUM"
CMF_CARDS_START_DATE = "20090401"


@dataclass(frozen=True)
class CmfTransactionCountRawObservation:
    dataset_code: str
    source_series_id: str
    source_codigo: str
    source_nombre: str
    institution_code: str
    institution_name: str
    period_month: date
    transaction_count: Decimal
    source_payload: dict[str, Any]

    def to_row(self) -> dict[str, Any]:
        return {
            "dataset_code": self.dataset_code,
            "source_series_id": self.source_series_id,
            "source_codigo": self.source_codigo,
            "source_nombre": self.source_nombre,
            "institution_code": self.institution_code,
            "institution_name": self.institution_name,
            "period_month": self.period_month.isoformat(),
            "transaction_count": str(self.transaction_count),
            "source_payload": self.source_payload,
        }


@dataclass(frozen=True)
class CmfTransactionCountCuratedObservation:
    institution_code: str
    institution_name: str
    period_month: date
    transaction_count: Decimal
    source_dataset_code: str

    def to_row(self) -> dict[str, str]:
        return {
            "institution_code": self.institution_code,
            "institution_name": self.institution_name,
            "period_month": self.period_month.isoformat(),
            "transaction_count": str(self.transaction_count),
            "source_dataset_code": self.source_dataset_code,
        }

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any


BANK_CREDIT_CARD_TRANSACTION_COUNT_DATASET = "bank_credit_card_transaction_count"
BANK_CREDIT_CARD_TRANSACTION_COUNT_TAG = "SBIF_TCRED_BANC_COMP_AGIFI_NUM"
BANK_CREDIT_CARD_PURCHASE_VOLUME_DATASET = "bank_credit_card_purchase_volume"
BANK_CREDIT_CARD_PURCHASE_VOLUME_TAG = "SBIF_TCRED_BANC_COMP_AGIFI_$"
BANK_CREDIT_CARD_TRANSACTION_COUNT_RAW_TABLE = "bank_credit_card_transaction_count_raw"
BANK_CREDIT_CARD_TRANSACTION_COUNT_CURATED_TABLE = "bank_credit_card_transaction_count_curated"
BANK_CREDIT_CARD_PURCHASE_VOLUME_RAW_TABLE = "bank_credit_card_purchase_volume_raw"
BANK_CREDIT_CARD_PURCHASE_VOLUME_CURATED_TABLE = "bank_credit_card_purchase_volume_curated"
BANK_CREDIT_CARD_PURCHASES_METRICS_TABLE = "bank_credit_card_purchases_metrics"
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


@dataclass(frozen=True)
class CmfPurchaseVolumeRawObservation:
    dataset_code: str
    source_series_id: str
    source_codigo: str
    source_nombre: str
    institution_code: str
    institution_name: str
    period_month: date
    nominal_volume_millions_clp: Decimal
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
            "nominal_volume_millions_clp": str(self.nominal_volume_millions_clp),
            "source_payload": self.source_payload,
        }


@dataclass(frozen=True)
class CmfPurchaseVolumeCuratedObservation:
    institution_code: str
    institution_name: str
    period_month: date
    nominal_volume_thousands_millions_clp: Decimal
    uf_date_used: date
    uf_value_used: Decimal
    real_volume_uf: Decimal
    source_dataset_code: str

    def to_row(self) -> dict[str, str]:
        return {
            "institution_code": self.institution_code,
            "institution_name": self.institution_name,
            "period_month": self.period_month.isoformat(),
            "nominal_volume_thousands_millions_clp": str(
                self.nominal_volume_thousands_millions_clp
            ),
            "uf_date_used": self.uf_date_used.isoformat(),
            "uf_value_used": str(self.uf_value_used),
            "real_volume_uf": str(self.real_volume_uf),
            "source_dataset_code": self.source_dataset_code,
        }


@dataclass(frozen=True)
class CmfPurchaseMetricsObservation:
    institution_code: str
    institution_name: str
    period_month: date
    nominal_volume_thousands_millions_clp: Decimal
    uf_date_used: date
    uf_value_used: Decimal
    real_volume_uf: Decimal
    transaction_count: Decimal
    average_ticket_uf: Decimal
    source_purchase_volume_dataset_code: str
    source_transaction_count_dataset_code: str

    def to_row(self) -> dict[str, str]:
        return {
            "institution_code": self.institution_code,
            "institution_name": self.institution_name,
            "period_month": self.period_month.isoformat(),
            "nominal_volume_thousands_millions_clp": str(
                self.nominal_volume_thousands_millions_clp
            ),
            "uf_date_used": self.uf_date_used.isoformat(),
            "uf_value_used": str(self.uf_value_used),
            "real_volume_uf": str(self.real_volume_uf),
            "transaction_count": str(self.transaction_count),
            "average_ticket_uf": str(self.average_ticket_uf),
            "source_purchase_volume_dataset_code": self.source_purchase_volume_dataset_code,
            "source_transaction_count_dataset_code": self.source_transaction_count_dataset_code,
        }

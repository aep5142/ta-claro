from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any


CMF_DATASETS_TABLE = "cmf_datasets"
CMF_DATASET_SYNC_STATE_TABLE = "cmf_dataset_sync_state"
BANK_DEBIT_CARD_OPS_RAW_TABLE = "bank_debit_card_ops_raw"
BANK_DEBIT_CARD_OPS_CURATED_TABLE = "bank_debit_card_ops_curated"
BANK_DEBIT_CARD_OPS_METRICS_VIEW = "bank_debit_card_ops_metrics"
BANK_DEBIT_CARD_COUNTS_RAW_TABLE = "bank_debit_card_counts_raw"
BANK_DEBIT_CARD_COUNTS_CURATED_TABLE = "bank_debit_card_counts_curated"
BANK_DEBIT_CARD_OPERATION_METRICS_VIEW = "bank_debit_card_operation_metrics"
CMF_MEASURE_KIND_TRANSACTION_COUNT = "transaction_count"
CMF_MEASURE_KIND_NOMINAL_VOLUME = "nominal_volume"
CMF_MEASURE_KIND_ACTIVE_CARDS_PRIMARY_DEBIT = "active_cards_primary_debit"
CMF_MEASURE_KIND_ACTIVE_CARDS_PRIMARY_ATM_ONLY = "active_cards_primary_atm_only"
CMF_MEASURE_KIND_ACTIVE_CARDS_SUPPLEMENTARY_DEBIT = "active_cards_supplementary_debit"
CMF_MEASURE_KIND_ACTIVE_CARDS_SUPPLEMENTARY_ATM_ONLY = (
    "active_cards_supplementary_atm_only"
)
CMF_MEASURE_KIND_ACTIVE_CARDS_TOTAL_DEBIT = "active_cards_total_debit"
CMF_MEASURE_KIND_ACTIVE_CARDS_TOTAL_ATM_ONLY = "active_cards_total_atm_only"
CMF_MEASURE_KIND_CARDS_WITH_OPERATIONS_DEBIT = "cards_with_operations_debit"
CMF_MEASURE_KIND_CARDS_WITH_OPERATIONS_ATM_ONLY = "cards_with_operations_atm_only"

BANK_DEBIT_CARD_OPS_DEBIT_TRANSACTIONS_DATASET = "bank_debit_card_ops_debit_transactions"
BANK_DEBIT_CARD_OPS_ATM_WITHDRAWALS_DATASET = "bank_debit_card_ops_atm_withdrawals"
BANK_DEBIT_CARD_OPS_DEBIT_TRANSACTIONS_TRANSACTION_COUNT_DATASET = (
    "bank_debit_card_ops_debit_transactions_transaction_count"
)
BANK_DEBIT_CARD_OPS_DEBIT_TRANSACTIONS_NOMINAL_VOLUME_DATASET = (
    "bank_debit_card_ops_debit_transactions_nominal_volume"
)
BANK_DEBIT_CARD_OPS_ATM_WITHDRAWALS_TRANSACTION_COUNT_DATASET = (
    "bank_debit_card_ops_atm_withdrawals_transaction_count"
)
BANK_DEBIT_CARD_OPS_ATM_WITHDRAWALS_NOMINAL_VOLUME_DATASET = (
    "bank_debit_card_ops_atm_withdrawals_nominal_volume"
)
BANK_DEBIT_CARD_COUNTS_DATASET = "bank_debit_card_counts"
BANK_DEBIT_CARD_ACTIVE_CARDS_PRIMARY_DEBIT_DATASET = (
    "bank_debit_card_active_cards_primary_debit"
)
BANK_DEBIT_CARD_ACTIVE_CARDS_PRIMARY_ATM_ONLY_DATASET = (
    "bank_debit_card_active_cards_primary_atm_only"
)
BANK_DEBIT_CARD_ACTIVE_CARDS_SUPPLEMENTARY_DEBIT_DATASET = (
    "bank_debit_card_active_cards_supplementary_debit"
)
BANK_DEBIT_CARD_ACTIVE_CARDS_SUPPLEMENTARY_ATM_ONLY_DATASET = (
    "bank_debit_card_active_cards_supplementary_atm_only"
)
BANK_DEBIT_CARD_ACTIVE_CARDS_TOTAL_DEBIT_DATASET = (
    "bank_debit_card_active_cards_total_debit"
)
BANK_DEBIT_CARD_ACTIVE_CARDS_TOTAL_ATM_ONLY_DATASET = (
    "bank_debit_card_active_cards_total_atm_only"
)
BANK_DEBIT_CARD_CARDS_WITH_OPERATIONS_DEBIT_DATASET = (
    "bank_debit_card_cards_with_operations_debit"
)
BANK_DEBIT_CARD_CARDS_WITH_OPERATIONS_ATM_ONLY_DATASET = (
    "bank_debit_card_cards_with_operations_atm_only"
)

BANK_DEBIT_CARD_OPERATION_DEBIT_TRANSACTIONS = "Debit Transactions"
BANK_DEBIT_CARD_OPERATION_ATM_WITHDRAWALS = "ATM Withdrawals"
BANK_DEBIT_CARD_OPERATION_RATE = "Total Activation Rate"

BANK_DEBIT_CARD_OPERATION_TYPES = (
    BANK_DEBIT_CARD_OPERATION_DEBIT_TRANSACTIONS,
    BANK_DEBIT_CARD_OPERATION_ATM_WITHDRAWALS,
)

CMF_DEBIT_CARDS_START_DATE = "20090401"


@dataclass(frozen=True)
class BankDebitCardEndpointConfig:
    operation_type: str
    dataset_code: str
    measure_kind: str
    source_tag: str
    source_nombre: str
    source_description: str
    source_endpoint_base: str
    refresh_frequency: str
    start_date: date
    is_active: bool = True

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "BankDebitCardEndpointConfig":
        return cls(
            operation_type=row["operation_type"],
            dataset_code=row["dataset_code"],
            measure_kind=row["measure_kind"],
            source_tag=row["source_tag"],
            source_nombre=row["source_nombre"],
            source_description=row["source_description"],
            source_endpoint_base=row["source_endpoint_base"],
            refresh_frequency=row["refresh_frequency"],
            start_date=date.fromisoformat(row["start_date"]),
            is_active=bool(row.get("is_active", True)),
        )


@dataclass(frozen=True)
class BankDebitCardOperationConfig:
    operation_type: str
    dataset_code: str
    transaction_count_dataset_code: str
    nominal_volume_dataset_code: str
    transaction_count_source_tag: str
    nominal_volume_source_tag: str
    source_nombre: str
    source_description: str
    source_endpoint_base: str
    refresh_frequency: str
    start_date: date


@dataclass(frozen=True)
class BankDebitCardCountsConfig:
    dataset_code: str
    active_cards_primary_debit_dataset_code: str
    active_cards_primary_atm_only_dataset_code: str
    active_cards_supplementary_debit_dataset_code: str
    active_cards_supplementary_atm_only_dataset_code: str
    active_cards_total_debit_dataset_code: str
    active_cards_total_atm_only_dataset_code: str
    cards_with_operations_debit_dataset_code: str
    cards_with_operations_atm_only_dataset_code: str
    active_cards_primary_debit_source_tag: str
    active_cards_primary_atm_only_source_tag: str
    active_cards_supplementary_debit_source_tag: str
    active_cards_supplementary_atm_only_source_tag: str
    active_cards_total_debit_source_tag: str
    active_cards_total_atm_only_source_tag: str
    cards_with_operations_debit_source_tag: str
    cards_with_operations_atm_only_source_tag: str
    source_endpoint_base: str
    refresh_frequency: str
    start_date: date


@dataclass(frozen=True)
class BankDebitCardOpsMeasureObservation:
    operation_type: str
    dataset_code: str
    source_series_id: str
    source_codigo: str
    source_nombre: str
    institution_code: str
    institution_name: str
    period_month: date
    value: Decimal
    source_payload: dict[str, Any]


@dataclass(frozen=True)
class BankDebitCardOpsRawObservation:
    operation_type: str
    dataset_code: str
    source_series_id: str
    source_codigo: str
    source_nombre: str
    institution_code: str
    institution_name: str
    period_month: date
    transaction_count: Decimal
    nominal_volume_millions_clp: Decimal
    source_payload: dict[str, Any]

    def to_row(self) -> dict[str, Any]:
        return {
            "operation_type": self.operation_type,
            "dataset_code": self.dataset_code,
            "source_series_id": self.source_series_id,
            "source_codigo": self.source_codigo,
            "source_nombre": self.source_nombre,
            "institution_code": self.institution_code,
            "institution_name": self.institution_name,
            "period_month": self.period_month.isoformat(),
            "transaction_count": str(self.transaction_count),
            "nominal_volume_millions_clp": str(self.nominal_volume_millions_clp),
            "source_payload": self.source_payload,
        }


@dataclass(frozen=True)
class BankDebitCardOpsCuratedObservation:
    operation_type: str
    dataset_code: str
    institution_code: str
    institution_name: str
    period_month: date
    transaction_count: Decimal
    nominal_volume_millions_clp: Decimal
    uf_date_used: date
    uf_value_used: Decimal
    real_value_uf: Decimal
    average_ticket_uf: Decimal
    total_active_cards: Decimal | None
    operations_per_active_card: Decimal | None
    source_dataset_code: str

    def to_row(self) -> dict[str, Any]:
        return {
            "operation_type": self.operation_type,
            "dataset_code": self.dataset_code,
            "institution_code": self.institution_code,
            "institution_name": self.institution_name,
            "period_month": self.period_month.isoformat(),
            "transaction_count": str(self.transaction_count),
            "nominal_volume_millions_clp": str(self.nominal_volume_millions_clp),
            "uf_date_used": self.uf_date_used.isoformat(),
            "uf_value_used": str(self.uf_value_used),
            "real_value_uf": str(self.real_value_uf),
            "average_ticket_uf": str(self.average_ticket_uf),
            "total_active_cards": (
                None if self.total_active_cards is None else str(self.total_active_cards)
            ),
            "operations_per_active_card": (
                None
                if self.operations_per_active_card is None
                else str(self.operations_per_active_card)
            ),
            "source_dataset_code": self.source_dataset_code,
        }


@dataclass(frozen=True)
class BankDebitCardCountObservation:
    dataset_code: str
    source_series_id: str
    source_codigo: str
    source_nombre: str
    institution_code: str
    institution_name: str
    period_month: date
    value: Decimal
    source_payload: dict[str, Any]


@dataclass(frozen=True)
class BankDebitCardCountRawObservation:
    dataset_code: str
    source_series_id: str
    source_codigo: str
    source_nombre: str
    institution_code: str
    institution_name: str
    period_month: date
    card_count: Decimal
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
            "card_count": str(self.card_count),
            "source_payload": self.source_payload,
        }


@dataclass(frozen=True)
class BankDebitCardCountsCuratedObservation:
    dataset_code: str
    institution_code: str
    institution_name: str
    period_month: date
    active_cards_primary: Decimal
    active_cards_supplementary: Decimal
    total_active_cards: Decimal
    total_cards_with_operations: Decimal
    operations_rate: Decimal | None

    def to_row(self) -> dict[str, Any]:
        return {
            "dataset_code": self.dataset_code,
            "institution_code": self.institution_code,
            "institution_name": self.institution_name,
            "period_month": self.period_month.isoformat(),
            "active_cards_primary": str(self.active_cards_primary),
            "active_cards_supplementary": str(self.active_cards_supplementary),
            "total_active_cards": str(self.total_active_cards),
            "total_cards_with_operations": str(self.total_cards_with_operations),
            "operations_rate": (
                None if self.operations_rate is None else str(self.operations_rate)
            ),
        }

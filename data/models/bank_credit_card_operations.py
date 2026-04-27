from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any


CMF_DATASETS_TABLE = "cmf_datasets"
CMF_DATASET_SYNC_STATE_TABLE = "cmf_dataset_sync_state"
BANK_CREDIT_CARD_OPS_RAW_TABLE = "bank_credit_card_ops_raw"
BANK_CREDIT_CARD_OPS_CURATED_TABLE = "bank_credit_card_ops_curated"
BANK_CREDIT_CARD_OPS_METRICS_VIEW = "bank_credit_card_ops_metrics"
CMF_MEASURE_KIND_TRANSACTION_COUNT = "transaction_count"
CMF_MEASURE_KIND_NOMINAL_VOLUME = "nominal_volume"

BANK_CREDIT_CARD_OPS_COMPRAS_DATASET = "bank_credit_card_ops_compras"
BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_DATASET = "bank_credit_card_ops_avance_en_efectivo"
BANK_CREDIT_CARD_OPS_CARGOS_POR_SERVICIO_DATASET = "bank_credit_card_ops_cargos_por_servicio"
BANK_CREDIT_CARD_OPS_COMPRAS_TRANSACTION_COUNT_DATASET = (
    "bank_credit_card_ops_compras_transaction_count"
)
BANK_CREDIT_CARD_OPS_COMPRAS_NOMINAL_VOLUME_DATASET = (
    "bank_credit_card_ops_compras_nominal_volume"
)
BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_TRANSACTION_COUNT_DATASET = (
    "bank_credit_card_ops_avance_en_efectivo_transaction_count"
)
BANK_CREDIT_CARD_OPS_AVANCE_EN_EFECTIVO_NOMINAL_VOLUME_DATASET = (
    "bank_credit_card_ops_avance_en_efectivo_nominal_volume"
)
BANK_CREDIT_CARD_OPS_CARGOS_POR_SERVICIO_TRANSACTION_COUNT_DATASET = (
    "bank_credit_card_ops_cargos_por_servicio_transaction_count"
)
BANK_CREDIT_CARD_OPS_CARGOS_POR_SERVICIO_NOMINAL_VOLUME_DATASET = (
    "bank_credit_card_ops_cargos_por_servicio_nominal_volume"
)

BANK_CREDIT_CARD_OPERATION_COMPRAS = "Compras"
BANK_CREDIT_CARD_OPERATION_AVANCE_EN_EFECTIVO = "Avance en Efectivo"
BANK_CREDIT_CARD_OPERATION_CARGOS_POR_SERVICIO = "Cargos por Servicio"

BANK_CREDIT_CARD_OPERATION_TYPES = (
    BANK_CREDIT_CARD_OPERATION_COMPRAS,
    BANK_CREDIT_CARD_OPERATION_AVANCE_EN_EFECTIVO,
    BANK_CREDIT_CARD_OPERATION_CARGOS_POR_SERVICIO,
)

CMF_CARDS_START_DATE = "20090401"


@dataclass(frozen=True)
class BankCreditCardEndpointConfig:
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
    def from_row(cls, row: dict[str, Any]) -> "BankCreditCardEndpointConfig":
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

    def to_dataset_row(self) -> dict[str, Any]:
        return {
            "operation_type": self.operation_type,
            "dataset_code": self.dataset_code,
            "measure_kind": self.measure_kind,
            "source_tag": self.source_tag,
            "source_nombre": self.source_nombre,
            "source_description": self.source_description,
            "source_endpoint_base": self.source_endpoint_base,
            "refresh_frequency": self.refresh_frequency,
            "start_date": self.start_date.isoformat(),
            "is_active": self.is_active,
        }

    def to_registry_row(self) -> dict[str, Any]:
        return self.to_dataset_row()


@dataclass(frozen=True)
class BankCreditCardOperationConfig:
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
class BankCreditCardOpsMeasureObservation:
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
class BankCreditCardOpsRawObservation:
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
class BankCreditCardOpsCuratedObservation:
    operation_type: str
    dataset_code: str
    institution_code: str
    institution_name: str
    period_month: date
    transaction_count: Decimal
    nominal_volume_thousands_millions_clp: Decimal
    uf_date_used: date
    uf_value_used: Decimal
    real_value_uf: Decimal
    average_ticket_uf: Decimal
    source_dataset_code: str

    def to_row(self) -> dict[str, Any]:
        return {
            "operation_type": self.operation_type,
            "dataset_code": self.dataset_code,
            "institution_code": self.institution_code,
            "institution_name": self.institution_name,
            "period_month": self.period_month.isoformat(),
            "transaction_count": str(self.transaction_count),
            "nominal_volume_thousands_millions_clp": str(
                self.nominal_volume_thousands_millions_clp
            ),
            "uf_date_used": self.uf_date_used.isoformat(),
            "uf_value_used": str(self.uf_value_used),
            "real_value_uf": str(self.real_value_uf),
            "average_ticket_uf": str(self.average_ticket_uf),
            "source_dataset_code": self.source_dataset_code,
        }

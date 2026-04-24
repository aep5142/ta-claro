from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class UfValue:
    uf_date: date
    value: float

    def to_row(self) -> dict[str, str | float]:
        return {
            "uf_date": self.uf_date.isoformat(),
            "value": self.value,
        }

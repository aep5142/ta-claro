from datetime import date, datetime

from data.models.uf import UfValue


def normalize_uf_date(raw_date: str) -> date:
    """Convert CMF date strings into date objects."""

    for date_format in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw_date, date_format).date()
        except ValueError:
            continue

    raise ValueError(f"Unsupported UF date format: {raw_date}")


def parse_uf_number(raw_value: str | int | float) -> float:
    """Convert CMF UF numbers, including Chilean-formatted strings, to float."""

    if isinstance(raw_value, int | float):
        return float(raw_value)

    normalized = raw_value.replace(".", "").replace(",", ".")
    return float(normalized)


def parse_uf_rows(payload: dict) -> list[UfValue]:
    values = [
        UfValue(
            uf_date=normalize_uf_date(entry["Fecha"]),
            value=parse_uf_number(entry["Valor"]),
        )
        for entry in payload["UFs"]
    ]
    return sorted(values, key=lambda value: value.uf_date)


def build_historical_uf_url(template: str, api_key: str, today: date | None = None) -> str:
    """Build the CMF historical UF URL from the existing environment template."""

    current_date = today or date.today()
    target_month_index = current_date.month + 2
    target_year = current_date.year + ((target_month_index - 1) // 12)
    target_month = ((target_month_index - 1) % 12) + 1

    return (
        template.replace("<format>", "json")
        .replace("<year>", str(target_year))
        .replace("<month>", str(target_month))
        .replace("<api_key>", api_key)
    )


async def fetch_historical_ufs(client, template: str, api_key: str) -> list[UfValue]:
    response = await client.get(build_historical_uf_url(template, api_key), timeout=30)
    response.raise_for_status()
    return parse_uf_rows(response.json())

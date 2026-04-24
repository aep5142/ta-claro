from datetime import date

import pytest

from data.sources.uf_source import build_historical_uf_url, parse_uf_rows


def test_parse_uf_rows_normalizes_dates_values_and_sorts():
    payload = {
        "UFs": [
            {"Fecha": "16-04-2026", "Valor": "39.251,23"},
            {"Fecha": "2026-04-15", "Valor": "39.200,10"},
        ]
    }

    values = parse_uf_rows(payload)

    assert [value.uf_date for value in values] == [
        date(2026, 4, 15),
        date(2026, 4, 16),
    ]
    assert [value.value for value in values] == [39200.10, 39251.23]


def test_parse_uf_rows_rejects_unknown_date_format():
    payload = {"UFs": [{"Fecha": "04.15.2026", "Valor": "39.200,10"}]}

    with pytest.raises(ValueError, match="Unsupported UF date format"):
        parse_uf_rows(payload)


def test_build_historical_uf_url_uses_next_month():
    url = build_historical_uf_url(
        "https://cmf.example/<year>/<month>/<format>?apikey=<api_key>",
        api_key="secret",
        today=date(2026, 4, 24),
    )

    assert url == "https://cmf.example/2026/5/json?apikey=secret"


def test_build_historical_uf_url_rolls_december_to_next_year():
    url = build_historical_uf_url(
        "https://cmf.example/<year>/<month>/<format>?apikey=<api_key>",
        api_key="secret",
        today=date(2026, 12, 24),
    )

    assert url == "https://cmf.example/2027/1/json?apikey=secret"

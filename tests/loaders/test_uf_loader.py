from datetime import date

from data.loaders.uf_loader import latest_stored_uf_date, new_uf_values
from data.models.uf import UfValue


class FakeResponse:
    def __init__(self, data):
        self.data = data


class FakeQuery:
    def __init__(self, response_data):
        self.response_data = response_data

    def select(self, *_args):
        return self

    def order(self, *_args, **_kwargs):
        return self

    def limit(self, *_args):
        return self

    def execute(self):
        return FakeResponse(self.response_data)


class FakeSupabase:
    def __init__(self, response_data):
        self.response_data = response_data

    def table(self, table_name):
        assert table_name == "uf_values"
        return FakeQuery(self.response_data)


def test_latest_stored_uf_date_returns_none_when_table_is_empty():
    assert latest_stored_uf_date(FakeSupabase([])) is None


def test_latest_stored_uf_date_reads_latest_row():
    sb = FakeSupabase([{"uf_date": "2026-04-15"}])

    assert latest_stored_uf_date(sb) == date(2026, 4, 15)


def test_new_uf_values_returns_only_rows_after_latest_stored_date():
    source_values = [
        UfValue(date(2026, 4, 14), 39100.0),
        UfValue(date(2026, 4, 15), 39200.0),
        UfValue(date(2026, 4, 16), 39300.0),
    ]

    assert new_uf_values(source_values, date(2026, 4, 15)) == [
        UfValue(date(2026, 4, 16), 39300.0)
    ]


def test_new_uf_values_returns_all_rows_when_no_stored_date_exists():
    source_values = [
        UfValue(date(2026, 4, 14), 39100.0),
        UfValue(date(2026, 4, 15), 39200.0),
    ]

    assert new_uf_values(source_values, None) == source_values

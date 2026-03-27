import sqlite3
import pandas as pd
import pytest

from ingestion.csv_loader import CSVLoader


@pytest.fixture
def conn():
    connection = sqlite3.connect(":memory:")
    yield connection
    connection.close()


def test_load_csv_creates_table_and_inserts_rows(tmp_path, conn):
    csv_path = tmp_path / "people.csv"
    df = pd.DataFrame(
        {
            "Name": ["Alice", "Bob"],
            "Age": [20, 21]
        }
    )
    df.to_csv(csv_path, index=False)

    loader = CSVLoader(conn)
    result = loader.load_csv(csv_path, "People")

    assert result["table_name"] == "people"
    assert result["rows_inserted"] == 2
    assert result["columns"] == [
        {"name": "name", "type": "TEXT"},
        {"name": "age", "type": "INTEGER"}
    ]

    rows = conn.execute('SELECT name, age FROM "people"').fetchall()
    assert rows == [("Alice", 20), ("Bob", 21)]


def test_load_csv_infers_column_types(tmp_path, conn):
    csv_path = tmp_path / "data.csv"
    df = pd.DataFrame(
        {
            "int": [1, 2, 3],
            "real": [1.5, 2.0, 3.2],
            "text": ["a", "b", "c"]
        }
    )
    df.to_csv(csv_path, index=False)

    loader = CSVLoader(conn)
    loader.load_csv(csv_path, "data")

    schema = conn.execute("PRAGMA table_info(data)").fetchall()
    types = {row[1]: row[2] for row in schema}

    assert types["int"] == "INTEGER"
    assert types["real"] == "REAL"
    assert types["text"] == "TEXT"


def test_load_csv_raises_if_file_does_not_exist(conn):
    loader = CSVLoader(conn)

    with pytest.raises(FileNotFoundError, match="CSV file not found"):
        loader.load_csv("missing_file.csv", "people")


def test_load_csv_appends_to_matching_table(tmp_path, conn):
    first_csv = tmp_path / "people1.csv"
    second_csv = tmp_path / "people2.csv"

    df1 = pd.DataFrame(
        {
            "Name": ["Alice"],
            "Age": [20]
        }
    )
    df2 = pd.DataFrame(
        {
            "Name": ["Bob"],
            "Age": [21]
        }
    )

    df1.to_csv(first_csv, index=False)
    df2.to_csv(second_csv, index=False)

    loader = CSVLoader(conn)
    loader.load_csv(first_csv, "people")
    result = loader.load_csv(second_csv, "people")

    assert result["table_name"] == "people"
    assert result["rows_inserted"] == 1
    assert result["action"] == "appended"

    rows = conn.execute('SELECT name, age FROM "people" ORDER BY id').fetchall()
    assert rows == [("Alice", 20), ("Bob", 21)]


def test_load_csv_raises_on_duplicate_column_names_after_normalization(tmp_path, conn):
    csv_path = tmp_path / "bad_columns.csv"
    df = pd.DataFrame(
        {
            "First Name": ["Alice"],
            "First_Name": ["Bob"]
        }
    )
    df.to_csv(csv_path, index=False)

    loader = CSVLoader(conn)

    with pytest.raises(ValueError, match="Duplicate column names"):
        loader.load_csv(csv_path, "people")


def test_load_csv_raises_if_csv_contains_id_column(tmp_path, conn):
    csv_path = tmp_path / "has_id.csv"
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "Name": ["Alice", "Bob"]
        }
    )
    df.to_csv(csv_path, index=False)

    loader = CSVLoader(conn)

    with pytest.raises(ValueError, match="column named 'id'"):
        loader.load_csv(csv_path, "people")


def test_load_csv_stores_missing_values_as_null(tmp_path, conn):
    csv_path = tmp_path / "missing_values.csv"
    df = pd.DataFrame(
        {
            "Name": ["Alice", None],
            "Age": [20, None]
        }
    )
    df.to_csv(csv_path, index=False)

    loader = CSVLoader(conn)
    loader.load_csv(csv_path, "people")

    rows = conn.execute(
        'SELECT name, age FROM "people" ORDER BY id').fetchall()
    assert rows == [("Alice", 20), (None, None)]


def test_load_csv_rejects_empty_table_name_after_normalization(tmp_path, conn):
    csv_path = tmp_path / "people.csv"
    df = pd.DataFrame(
        {
            "Name": ["Alice"],
            "Age": [20]
        }
    )
    df.to_csv(csv_path, index=False)

    loader = CSVLoader(conn)

    with pytest.raises(ValueError, match="Table name is empty after normalization"):
        loader.load_csv(csv_path, "!")


def test_load_csv_rejects_reserved_sqlite_sequence_name(tmp_path, conn):
    csv_path = tmp_path / "people.csv"
    df = pd.DataFrame(
        {
            "Name": ["Alice"],
            "Age": [20]
        }
    )
    df.to_csv(csv_path, index=False)

    loader = CSVLoader(conn)

    with pytest.raises(ValueError, match="Invalid table name"):
        loader.load_csv(csv_path, "sqlite_sequence")

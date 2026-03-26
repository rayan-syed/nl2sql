import sqlite3
import pandas as pd

from ingestion.csv_loader import CSVLoader


def test_load_csv_creates_table_and_inserts_rows(tmp_path):
    conn = sqlite3.connect(":memory:")

    csv_path = tmp_path / "people.csv"
    df = pd.DataFrame({
        "Name": ["Alice", "Bob"],
        "Age": [20, 21]
    })
    df.to_csv(csv_path, index=False)

    loader = CSVLoader(conn)
    result = loader.load_csv(csv_path, "People")

    assert result["table_name"] == "people"
    assert result["rows_inserted"] == 2
    assert result["columns"] == ["name", "age"]

    rows = conn.execute("SELECT name, age FROM people").fetchall()
    assert rows == [("Alice", 20), ("Bob", 21)]


def test_load_csv_infers_column_types(tmp_path):
    conn = sqlite3.connect(":memory:")

    csv_path = tmp_path / "data.csv"
    df = pd.DataFrame({
        "int": [1, 2, 3],
        "real": [1.0, 2.0, 3.0],
        "text": ["a", "b", "c"]
    })
    df.to_csv(csv_path, index=False)

    loader = CSVLoader(conn)
    loader.load_csv(csv_path, "data")

    schema = conn.execute("PRAGMA table_info(data)").fetchall()
    types = {row[1]: row[2] for row in schema}

    assert types["int"] == "INTEGER"
    assert types["real"] == "REAL"
    assert types["text"] == "TEXT"

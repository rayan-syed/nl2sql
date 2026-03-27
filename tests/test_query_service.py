import sqlite3

import pytest

from query.query_service import QueryService


@pytest.fixture
def conn():
    connection = sqlite3.connect(":memory:")
    connection.execute("""
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            age INTEGER,
            salary REAL
        )
    """)
    connection.execute("""
        INSERT INTO employees (name, age, salary)
        VALUES
            ('Alice', 20, 100000.0),
            ('Bob', 21, 90000.0)
    """)
    connection.commit()

    yield connection
    connection.close()


@pytest.fixture
def query_service(conn):
    return QueryService(conn)


def test_run_sql_query_returns_rows_for_valid_query(query_service):
    result = query_service.run_sql_query(
        "SELECT name, age FROM employees ORDER BY id"
    )

    assert result["success"] is True
    assert result["columns"] == ["name", "age"]
    assert result["rows"] == [("Alice", 20), ("Bob", 21)]
    assert result["row_count"] == 2


def test_run_sql_query_rejects_invalid_query(query_service):
    result = query_service.run_sql_query(
        "DELETE FROM employees"
    )

    assert result["success"] is False
    assert result["error"] == "Only SELECT queries are allowed."


def test_run_sql_query_rejects_unknown_table(query_service):
    result = query_service.run_sql_query(
        "SELECT name FROM missing_table"
    )

    assert result["success"] is False
    assert result["error"] == "Unknown table: missing_table"


def test_run_sql_query_rejects_unknown_column(query_service):
    result = query_service.run_sql_query(
        "SELECT missing_column FROM employees"
    )

    assert result["success"] is False
    assert result["error"] == "Unknown column: missing_column"

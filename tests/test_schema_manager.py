import sqlite3

import pytest

from schema.schema_manager import SchemaManager


@pytest.fixture
def conn():
    connection = sqlite3.connect(":memory:")
    yield connection
    connection.close()


@pytest.fixture
def schema_manager(conn):
    return SchemaManager(conn)


def test_list_tables_returns_user_tables(conn, schema_manager):
    conn.execute("""
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            age INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            price REAL
        )
    """)
    conn.commit()

    tables = schema_manager.list_tables()

    assert tables == ["employees", "products"]


def test_get_table_schema_returns_expected_columns(conn, schema_manager):
    conn.execute("""
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            age INTEGER
        )
    """)
    conn.commit()

    schema = schema_manager.get_table_schema("employees")

    assert schema["table_name"] == "employees"
    assert schema["columns"] == [
        {"name": "id", "type": "INTEGER"},
        {"name": "name", "type": "TEXT"},
        {"name": "age", "type": "INTEGER"}
    ]


def test_schemas_match_returns_true_for_matching_schema(conn, schema_manager):
    conn.execute("""
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            age INTEGER
        )
    """)
    conn.commit()

    csv_columns = [
        {"name": "name", "type": "TEXT"},
        {"name": "age", "type": "INTEGER"}
    ]

    assert schema_manager.schemas_match(csv_columns, "employees") is True


def test_schemas_match_returns_false_for_different_schema(conn, schema_manager):
    conn.execute("""
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            age INTEGER
        )
    """)
    conn.commit()

    csv_columns = [
        {"name": "name", "type": "TEXT"},
        {"name": "salary", "type": "REAL"}
    ]

    assert schema_manager.schemas_match(csv_columns, "employees") is False


def test_find_matching_table_returns_matching_table_name(conn, schema_manager):
    conn.execute("""
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            age INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT
        )
    """)
    conn.commit()

    csv_columns = [
        {"name": "name", "type": "TEXT"},
        {"name": "age", "type": "INTEGER"}
    ]

    assert schema_manager.find_matching_table(csv_columns) == "employees"


def test_format_schema_for_prompt_returns_readable_text(conn, schema_manager):
    conn.execute("""
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            age INTEGER
        )
    """)
    conn.commit()

    formatted = schema_manager.format_schema_for_prompt()

    assert "employees: id (INTEGER), name (TEXT), age (INTEGER)" in formatted


def test_get_table_schema_raises_for_missing_table(schema_manager):
    with pytest.raises(ValueError, match="does not exist"):
        schema_manager.get_table_schema("missing_table")

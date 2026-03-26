import sqlite3

import pytest

from validation.sql_validator import SQLValidator


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
        CREATE TABLE departments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            department_name TEXT
        )
    """)
    connection.commit()
    yield connection
    connection.close()


@pytest.fixture
def validator(conn):
    return SQLValidator(conn)


def test_validate_accepts_valid_select_query(validator):
    is_valid, error = validator.validate("SELECT name, age FROM employees")

    assert is_valid is True
    assert error is None


def test_validate_accepts_select_star(validator):
    is_valid, error = validator.validate("SELECT * FROM employees")

    assert is_valid is True
    assert error is None


def test_validate_rejects_non_select_query(validator):
    is_valid, error = validator.validate("DELETE FROM employees")

    assert is_valid is False
    assert error == "Only SELECT queries are allowed."


def test_validate_rejects_unknown_table(validator):
    is_valid, error = validator.validate("SELECT name FROM missing_table")

    assert is_valid is False
    assert error == "Unknown table: missing_table"


def test_validate_rejects_unknown_column(validator):
    is_valid, error = validator.validate("SELECT missing_column FROM employees")

    assert is_valid is False
    assert error == "Unknown column: missing_column"


def test_validate_rejects_multiple_statements(validator):
    is_valid, error = validator.validate("SELECT name FROM employees; DROP TABLE employees;")

    assert is_valid is False
    assert error == "Only one SQL statement is allowed."


def test_validate_accepts_qualified_column_names(validator):
    is_valid, error = validator.validate("SELECT employees.name FROM employees")

    assert is_valid is True
    assert error is None


def test_validate_accepts_simple_function_calls(validator):
    is_valid, error = validator.validate("SELECT COUNT(*) FROM employees")

    assert is_valid is True
    assert error is None

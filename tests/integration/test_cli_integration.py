import sqlite3
import pandas as pd

from cli import CLI


class MockLLMAdapter:
    def generate_sql(self, user_query, schema_text):
        return "SELECT name, age FROM employees ORDER BY id"


def test_cli_load_and_query_integration(tmp_path, monkeypatch, capsys):
    csv_path = tmp_path / "employees.csv"
    df = pd.DataFrame(
        {
            "Name": ["Alice", "Bob"],
            "Age": [20, 21],
            "Salary": [100000, 90000]
        }
    )
    df.to_csv(csv_path, index=False)

    inputs = iter([
        "load",
        str(csv_path),
        "employees",
        "query",
        "SELECT name, age FROM employees ORDER BY id",
        "exit"
    ])

    monkeypatch.setattr("builtins.input", lambda _: next(inputs))

    conn = sqlite3.connect(":memory:")
    cli = CLI(conn)
    cli.run()

    captured = capsys.readouterr()
    output = captured.out

    assert 'Created table "employees" with 2 rows.' in output
    assert "name | age" in output
    assert "Alice | 20" in output
    assert "Bob | 21" in output
    assert "2 row(s) returned." in output

    rows = conn.execute('SELECT name, age FROM "employees" ORDER BY id').fetchall()
    assert rows == [("Alice", 20), ("Bob", 21)]

    conn.close()


def test_cli_rejects_invalid_query_integration(monkeypatch, capsys):
    inputs = iter([
        "query",
        "DELETE FROM employees",
        "exit"
    ])

    monkeypatch.setattr("builtins.input", lambda _: next(inputs))

    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        )
    """)
    conn.commit()

    cli = CLI(conn)
    cli.run()

    captured = capsys.readouterr()
    output = captured.out

    assert "Error: Only SELECT queries are allowed." in output

    conn.close()


def test_cli_ask_integration(tmp_path, monkeypatch, capsys):
    csv_path = tmp_path / "employees.csv"
    df = pd.DataFrame(
        {
            "Name": ["Alice", "Bob"],
            "Age": [20, 21]
        }
    )
    df.to_csv(csv_path, index=False)

    inputs = iter([
        "load",
        str(csv_path),
        "employees",
        "ask",
        "Show me employee names and ages",
        "exit"
    ])

    monkeypatch.setattr("builtins.input", lambda _: next(inputs))

    conn = sqlite3.connect(":memory:")
    cli = CLI(conn, llm_adapter=MockLLMAdapter())
    cli.run()

    captured = capsys.readouterr()
    output = captured.out

    assert 'Created table "employees" with 2 rows.' in output
    assert "Generated SQL:" in output
    assert "SELECT name, age FROM employees ORDER BY id" in output
    assert "name | age" in output
    assert "Alice | 20" in output
    assert "Bob | 21" in output

    conn.close()

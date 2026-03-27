import sqlite3

from cli import CLI
from llm.gemini_adapter import GeminiAdapter


def main():
    conn = sqlite3.connect("nl2sql.db")

    try:
        llm_adapter = GeminiAdapter()
    except Exception as err:
        print(f"LLM not available: {err}")
        llm_adapter = None

    cli = CLI(conn, llm_adapter=llm_adapter)

    try:
        cli.run()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

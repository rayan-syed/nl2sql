import sqlite3

from cli import CLI


def main():
    conn = sqlite3.connect("nl2sql.db")
    cli = CLI(conn)

    try:
        cli.run()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

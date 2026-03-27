from ingestion.csv_loader import CSVLoader
from query.query_service import QueryService
from schema.schema_manager import SchemaManager


class CLI:
    def __init__(self, conn, llm_adapter=None):
        self.conn = conn
        self.csv_loader = CSVLoader(conn)
        self.query_service = QueryService(conn, llm_adapter=llm_adapter)
        self.schema_manager = SchemaManager(conn)

    def run(self):
        print("Welcome to nl2sql")
        print("Type 'help' to see available commands.")

        while True:
            command = input("\n> ").strip()

            if not command:
                continue

            if command == "exit":
                break

            if command == "help":
                self.print_help()
                continue

            if command == "tables":
                self.show_tables()
                continue

            if command == "schema":
                self.show_schema()
                continue

            if command == "load":
                self.load_csv()
                continue

            if command == "query":
                self.run_sql_query()
                continue

            if command == "ask":
                self.run_nl_query()
                continue

            print("Unknown command. Type 'help' to see available commands.")

    def print_help(self):
        print("\nAvailable commands:")
        print("  load   - load a CSV file into the database")
        print("  tables - list tables in the database")
        print("  schema - show database schema")
        print("  query  - run a SQL query")
        print("  ask    - ask a natural language question")
        print("  help   - show this message")
        print("  exit   - quit the program")

    def show_tables(self):
        tables = self.schema_manager.list_tables()

        if not tables:
            print("No tables found.")
            return

        print("\nTables:")
        for table in tables:
            print(f"  - {table}")

    def show_schema(self):
        schema_text = self.schema_manager.format_schema_for_prompt()
        print(f"{schema_text}")

    def load_csv(self):
        csv_path = input("CSV path: ").strip()
        table_name = input("Table name: ").strip()

        try:
            result = self.csv_loader.load_csv(csv_path, table_name)
            print(
                f'{result["action"].capitalize()} table "{result["table_name"]}" '
                f'with {result["rows_inserted"]} rows.'
            )
        except Exception as e:
            print(f"Error: {e}")

    def run_sql_query(self):
        sql = input("SQL query: ").strip()
        result = self.query_service.run_sql_query(sql)

        if not result["success"]:
            print(f'Error: {result["error"]}')
            return

        self.print_query_result(result)

    def run_nl_query(self):
        user_query = input("Question: ").strip()
        result = self.query_service.run_nl_query(user_query)

        if "generated_sql" in result:
            print(f'\nGenerated SQL:\n{result["generated_sql"]}\n')

        if not result["success"]:
            print(f'Error: {result["error"]}')
            return

        self.print_query_result(result)

    def print_query_result(self, result):
        columns = result["columns"]
        rows = result["rows"]

        if not rows:
            print("Query succeeded. No rows returned.")
            return

        print()
        print(" | ".join(columns))
        print("-" * len(" | ".join(columns)))

        for row in rows:
            print(" | ".join(str(value) for value in row))

        print(f'\n{result["row_count"]} row(s) returned.')

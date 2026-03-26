LIST_TABLES_QUERY = """
SELECT name
FROM sqlite_master
WHERE type='table' AND name NOT LIKE 'sqlite_%'
ORDER BY name
"""

TABLE_EXISTS_QUERY = """
SELECT name
FROM sqlite_master
WHERE type='table' AND name=?
"""


class SchemaManager:
    def __init__(self, conn):
        self.conn = conn

    def list_tables(self):
        rows = self.conn.execute(LIST_TABLES_QUERY).fetchall()
        return [row[0] for row in rows]

    def table_exists(self, table_name):
        row = self.conn.execute(TABLE_EXISTS_QUERY, (table_name,)).fetchone()
        return row is not None

    def get_table_schema(self, table_name):
        if not self.table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist.")

        query = f'PRAGMA table_info("{table_name}")'
        rows = self.conn.execute(query).fetchall()

        columns = []
        for row in rows:
            columns.append(
                {
                    "name": row[1],
                    "type": row[2]
                }
            )

        return {
            "table_name": table_name,
            "columns": columns
        }

    def normalize_schema_columns(self, columns):
        normalized = []
        for column in columns:
            normalized.append(
                {
                    "name": column["name"].strip().lower(),
                    "type": column["type"].strip().upper()
                }
            )
        return normalized

    def schemas_match(self, csv_columns, table_name):
        table_schema = self.get_table_schema(table_name)

        existing_columns = []
        for col in table_schema["columns"]:
            if col["name"] != "id":
                existing_columns.append(col)

        normalized_csv = self.normalize_schema_columns(csv_columns)
        normalized_existing = self.normalize_schema_columns(existing_columns)

        return normalized_csv == normalized_existing

    def find_matching_table(self, csv_columns):
        for table_name in self.list_tables():
            if self.schemas_match(csv_columns, table_name):
                return table_name
        return None

    def format_schema_for_prompt(self):
        tables = self.list_tables()

        if not tables:
            return "No tables found in the database."

        lines = []
        for table_name in tables:
            schema = self.get_table_schema(table_name)
            parts = []

            for column in schema["columns"]:
                parts.append(f'{column["name"]} ({column["type"]})')

            lines.append(f'{table_name}: ' + ", ".join(parts))

        return "\n".join(lines)

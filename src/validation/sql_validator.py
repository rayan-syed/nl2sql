import re

from schema.schema_manager import SchemaManager


FORBIDDEN_KEYWORDS = {
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "replace",
    "truncate",
    "attach",
    "detach",
    "pragma"
}


class SQLValidator:
    def __init__(self, conn):
        self.conn = conn
        self.schema_manager = SchemaManager(conn)

    def validate(self, sql):
        if not isinstance(sql, str) or not sql.strip():
            return False, "SQL query cannot be empty."

        sql = sql.strip()

        if sql.count(";") > 1:
            return False, "Only one SQL statement is allowed."

        if ";" in sql[:-1]:
            return False, "Only one SQL statement is allowed."

        sql_no_semicolon = sql[:-1].strip() if sql.endswith(";") else sql

        if not sql_no_semicolon.lower().startswith("select"):
            return False, "Only SELECT queries are allowed."

        lowered = sql_no_semicolon.lower()
        for keyword in FORBIDDEN_KEYWORDS:
            if re.search(rf"\b{keyword}\b", lowered):
                return False, f"Forbidden SQL keyword detected: {keyword.upper()}"

        table_names = self.extract_table_names(sql_no_semicolon)
        if not table_names:
            return False, "Query must reference at least one table."

        for table_name in table_names:
            if not self.schema_manager.table_exists(table_name):
                return False, f"Unknown table: {table_name}"

        selected_columns = self.extract_selected_columns(sql_no_semicolon)

        if selected_columns is None:
            return False, "Could not parse selected columns."

        if "*" in selected_columns:
            return True, None

        valid_columns = self.get_valid_columns_for_tables(table_names)

        for column in selected_columns:
            if self.is_function_call(column):
                continue

            cleaned = self.clean_column_name(column)

            if "." in cleaned:
                table_name, column_name = cleaned.split(".", 1)

                if table_name not in table_names:
                    return False, f"Unknown table in column reference: {table_name}"

                table_schema = self.schema_manager.get_table_schema(table_name)
                table_columns = {col["name"] for col in table_schema["columns"]}

                if column_name not in table_columns:
                    return False, f"Unknown column: {cleaned}"
            else:
                if cleaned not in valid_columns:
                    return False, f"Unknown column: {cleaned}"

        return True, None

    def extract_table_names(self, sql):
        matches = re.findall(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql, re.IGNORECASE)
        return [match.lower() for match in matches]

    def extract_selected_columns(self, sql):
        match = re.search(
            r"select\s+(.*?)\s+from\s",
            sql,
            re.IGNORECASE | re.DOTALL
        )

        if not match:
            return None

        column_text = match.group(1).strip()
        if not column_text:
            return None

        columns = [col.strip() for col in column_text.split(",")]
        return columns

    def get_valid_columns_for_tables(self, table_names):
        valid_columns = set()

        for table_name in table_names:
            schema = self.schema_manager.get_table_schema(table_name)
            for column in schema["columns"]:
                valid_columns.add(column["name"])

        return valid_columns

    def is_function_call(self, column):
        return "(" in column and ")" in column

    def clean_column_name(self, column):
        column = re.sub(r"\s+as\s+.+$", "", column, flags=re.IGNORECASE)
        column = column.strip()

        if " " in column:
            column = column.split()[0]

        return column.lower()

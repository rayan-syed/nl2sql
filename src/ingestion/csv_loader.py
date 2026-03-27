import os
import re
import sqlite3
import pandas as pd

from schema.schema_manager import SchemaManager


CREATE_TABLE_TEMPLATE = """
CREATE TABLE "{table_name}" (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    {columns}
)
"""

INSERT_TEMPLATE = """
INSERT INTO "{table_name}" ({columns})
VALUES ({placeholders})
"""


class CSVLoader:
    def __init__(self, conn):
        self.conn = conn
        self.schema_manager = SchemaManager(conn)

    def normalize_name(self, name):
        name = str(name).strip().lower()
        name = re.sub(r"[^a-zA-Z0-9]+", "_", name)
        name = re.sub(r"_+", "_", name)
        name = name.strip("_")
        return name

    def infer_type(self, series):
        non_null = series.dropna()

        if len(non_null) == 0:
            return "TEXT"

        if pd.api.types.is_integer_dtype(non_null):
            return "INTEGER"

        if pd.api.types.is_float_dtype(non_null):
            if (non_null % 1 == 0).all():
                return "INTEGER"
            return "REAL"

        return "TEXT"

    def load_csv(self, csv_path, table_name):
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        table_name = self.normalize_name(table_name)
        if not table_name:
            raise ValueError("Table name is empty after normalization.")

        if table_name == "sqlite_sequence":
            raise ValueError("Invalid table name.")

        df = pd.read_csv(csv_path)

        if len(df.columns) == 0:
            raise ValueError("CSV file has no columns.")

        normalized_columns = [self.normalize_name(col) for col in df.columns]

        if any(col == "" for col in normalized_columns):
            raise ValueError("One or more column names are empty after normalization.")

        if "id" in normalized_columns:
            raise ValueError(
                "CSV cannot contain a column named 'id' because the table adds its own primary key."
            )

        if len(normalized_columns) != len(set(normalized_columns)):
            raise ValueError("Duplicate column names found after normalization.")

        df.columns = normalized_columns

        column_info = []
        column_defs = []

        for col in df.columns:
            sql_type = self.infer_type(df[col])
            column_info.append({"name": col, "type": sql_type})
            column_defs.append(f'"{col}" {sql_type}')

        matching_table = self.schema_manager.find_matching_table(column_info)

        if matching_table is not None:
            final_table_name = matching_table
            create_new_table = False
        elif self.schema_manager.table_exists(table_name):
            raise ValueError(
                f"Table '{table_name}' already exists but its schema does not match the CSV."
            )
        else:
            final_table_name = table_name
            create_new_table = True

        quoted_columns = ", ".join([f'"{col}"' for col in df.columns])
        placeholders = ", ".join(["?"] * len(df.columns))

        create_sql = CREATE_TABLE_TEMPLATE.format(
            table_name=final_table_name,
            columns=", ".join(column_defs)
        )

        insert_sql = INSERT_TEMPLATE.format(
            table_name=final_table_name,
            columns=quoted_columns,
            placeholders=placeholders
        )

        rows = []
        for row in df.itertuples(index=False, name=None):
            cleaned_row = []
            for value in row:
                if pd.isna(value):
                    cleaned_row.append(None)
                else:
                    cleaned_row.append(value)
            rows.append(tuple(cleaned_row))

        try:
            if create_new_table:
                self.conn.execute(create_sql)

            for row in rows:
                self.conn.execute(insert_sql, row)

            self.conn.commit()
        except sqlite3.Error as e:
            self.conn.rollback()
            raise ValueError(f"Failed to load CSV into database: {e}")

        action = "created" if create_new_table else "appended"

        return {
            "table_name": final_table_name,
            "rows_inserted": len(df),
            "columns": column_info,
            "action": action
        }

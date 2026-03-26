import re
import pandas as pd


class CSVLoader:
    def __init__(self, conn):
        self.conn = conn

    def normalize_name(self, name):
        name = name.strip().lower()
        name = re.sub(r"\s+", "_", name)
        name = re.sub(r"[^a-zA-Z0-9_]", "", name)
        return name

    def infer_type(self, series):
        if pd.api.types.is_integer_dtype(series):
            return "INTEGER"
        elif pd.api.types.is_float_dtype(series):
            return "REAL"
        else:
            return "TEXT"

    def load_csv(self, csv_path, table_name):
        df = pd.read_csv(csv_path)

        table_name = self.normalize_name(table_name)
        df.columns = [self.normalize_name(col) for col in df.columns]

        columns = []
        for col in df.columns:
            sql_type = self.infer_type(df[col])
            columns.append(f"{col} {sql_type}")

        create_sql = f"""
        CREATE TABLE {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {", ".join(columns)}
        )
        """
        self.conn.execute(create_sql)

        placeholders = ", ".join(["?"] * len(df.columns))
        insert_sql = f"""
        INSERT INTO {table_name} ({", ".join(df.columns)})
        VALUES ({placeholders})
        """

        for row in df.itertuples(index=False, name=None):
            self.conn.execute(insert_sql, row)

        self.conn.commit()

        return {
            "table_name": table_name,
            "rows_inserted": len(df),
            "columns": list(df.columns),
        }

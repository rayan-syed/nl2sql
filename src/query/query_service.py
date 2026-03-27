from validation.sql_validator import SQLValidator


class QueryService:
    def __init__(self, conn):
        self.conn = conn
        self.validator = SQLValidator(conn)

    def run_sql_query(self, sql):
        is_valid, error = self.validator.validate(sql)

        if not is_valid:
            return {
                "success": False,
                "error": error
            }

        try:
            cursor = self.conn.execute(sql)
            rows = cursor.fetchall()

            columns = []
            if cursor.description is not None:
                columns = [col[0] for col in cursor.description]

            return {
                "success": True,
                "columns": columns,
                "rows": rows,
                "row_count": len(rows)
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"Query execution failed: {e}"
            }

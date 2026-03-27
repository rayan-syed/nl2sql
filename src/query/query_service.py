from validation.sql_validator import SQLValidator
from schema.schema_manager import SchemaManager


class QueryService:
    def __init__(self, conn, llm_adapter=None):
        self.conn = conn
        self.validator = SQLValidator(conn)
        self.schema_manager = SchemaManager(conn)
        self.llm_adapter = llm_adapter

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

    def run_nl_query(self, user_query):
        if self.llm_adapter is None:
            return {
                "success": False,
                "error": "LLM support is not configured."
            }

        schema_text = self.schema_manager.format_schema_for_prompt()

        try:
            generated_sql = self.llm_adapter.generate_sql(user_query, schema_text)
        except Exception as e:
            return {
                "success": False,
                "error": f"LLM generation failed: {e}"
            }

        result = self.run_sql_query(generated_sql)
        result["generated_sql"] = generated_sql
        return result

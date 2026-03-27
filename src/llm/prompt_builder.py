def build_nl_to_sql_prompt(user_query, schema_text):
    return f"""
You are generating exactly one SQLite SELECT query.

Database schema:
{schema_text}

User request:
{user_query}

Rules:
- Return exactly one SQL SELECT query
- Do not include markdown
- Do not include explanation
- Use only the tables and columns shown in the schema
- The query must be valid SQLite
""".strip()

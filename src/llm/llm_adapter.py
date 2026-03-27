# abstract base class for LLM adapters (in case I want to support both local LLMs & APIs)
class LLMAdapter:
    def generate_sql(self, user_query, schema_text):
        raise NotImplementedError("Subclasses must implement generate_sql().")

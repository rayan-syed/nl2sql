import os

from google import genai

from llm.llm_adapter import LLMAdapter
from llm.prompt_builder import build_nl_to_sql_prompt


class GeminiAdapter(LLMAdapter):
    def __init__(self, model="gemini-2.5-flash"):
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY not set")

        self.client = genai.Client(api_key=api_key)
        self.model = model

    def generate_sql(self, user_query, schema_text):
        prompt = build_nl_to_sql_prompt(user_query, schema_text)

        response = self.client.models.generate_content(
            model=self.model,
            contents=prompt,
        )

        text = response.text or ""
        return self.clean_output(text)

    def clean_output(self, text):
        text = text.strip()

        if text.startswith("```"):
            lines = text.splitlines()
            if len(lines) >= 3:
                text = "\n".join(lines[1:-1]).strip()

        if text.lower().startswith("sql"):
            text = text[3:].strip()

        return text.strip()

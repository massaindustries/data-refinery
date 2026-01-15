"""
Structuring Agent - Step 1 of the pipeline.
Segments text into sections and extracts preliminary fields.
"""

import json
import re
from typing import Any

from config import MODEL_STRUCTURING
from regolo_client import RegoloClient, generate_deterministic_id


STRUCTURING_SYSTEM_PROMPT = """You are a Structuring Agent for Italian document data extraction.

Your task is to:
1. Segment the document text into sections (ANAGRAFICA, AMMINISTRATIVI, TRANSAZIONI, TICKET, ALTRO)
2. Extract and classify data fields from each section
3. Clean text from encoding errors, special characters, and normalize whitespace

Section definitions:
- ANAGRAFICA: Customer personal data (nome, cognome, email, telefono, indirizzo, codice fiscale, etc.)
- AMMINISTRATIVI: Policy/administrative documents (polizza number, dates, amounts, status, etc.)
- TRANSAZIONI: Financial transactions (payments, refunds, amounts, dates, etc.)
- TICKET: Support tickets (ticket IDs, descriptions, status, dates, etc.)
- ALTRO: Anything that doesn't fit above categories

For each section found:
- Identify the section type
- Extract all relevant fields as key-value pairs
- Clean extracted values:
  - Remove encoding errors
  - Normalize whitespace (single spaces, no leading/trailing)
  - Fix common OCR errors in Italian text

Return a JSON object with this structure:
{
  "sections": [
    {
      "type": "ANAGRAFICA|AMMINISTRATIVI|TRANSAZIONI|TICKET|ALTRO",
      "page": 1,
      "raw_text": "original extracted text",
      "confidence": 0.85,
      "fields": {
        "field_name": "cleaned_value"
      }
    }
  ],
  "extracted_fields": {
    "customers": [...],
    "policies": [...],
    "transactions": [...],
    "tickets": [...]
  },
  "overall_confidence": 0.82,
  "warnings": ["any extraction warnings"]
}

Field naming: Use snake_case Italian field names"""


class StructuringAgent:
    def __init__(self, client: RegoloClient = None):
        self.client = client or RegoloClient()
        self.model = MODEL_STRUCTURING

    def process(self, raw_text: str) -> dict:
        user_content = f"""Process this document and extract structured data.

DOCUMENT TEXT:
{raw_text}

Return the structured JSON output as specified."""

        response, error = self.client.call_with_retry(
            system_prompt=STRUCTURING_SYSTEM_PROMPT,
            user_content=user_content,
            model=self.model
        )

        if error:
            return {"error": error, "success": False}

        try:
            message = response["choices"][0]["message"]
            content = message.get("content") or message.get("reasoning_content", "")
            if not content:
                return {"error": "Empty response content from API", "success": False}
            content = self._extract_json(content)
            result = json.loads(content)
            result["success"] = True
            return result
        except Exception as e:
            return {"error": f"Parse error: {str(e)}", "success": False, "raw_response": str(response)[:500]}

    def _extract_json(self, text: str) -> str:
        text = text.strip()
        if text.startswith("```json"):
            text = text[7:]
        if text.startswith("```"):
            text = text[3:]
        if text.endswith("```"):
            text = text[:-3]
        return text.strip()


def run_structuring_agent(raw_text: str) -> dict:
    agent = StructuringAgent()
    return agent.process(raw_text)

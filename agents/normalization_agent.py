"""
Normalization Agent - Step 2 of the pipeline.
Normalizes dates, amounts, currencies, and other fields.
"""

import json
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from config import MODEL_NORMALIZATION
from regolo_client import RegoloClient


NORMALIZATION_SYSTEM_PROMPT = """You are a Normalization Agent for Italian document data.

Your task is to normalize extracted data to standard formats:

1. DATES → ISO 8601 (YYYY-MM-DD)
   Input formats to handle:
   - "13/01/24" → "2024-01-13"
   - "13-01-2024" → "2024-01-13"
   - "13 gennaio 2024" → "2024-01-13"
   - "01/2024" → "2024-01-01" (use first day of month)
   - "gennaio 2024" → "2024-01-01"
   - "20 gen 2024" → "2024-01-20"

2. AMOUNTS → Canonical decimal format
   - "1.200" → "1200.00"
   - "1200.00" → "1200.00"
   - "€ 1.200,00" → "1200.00"
   - "€1.200,00" → "1200.00"
   - "Euro 1200,00" → "1200.00"
   - Remove currency symbols, normalize decimal separators
   - Use . as decimal separator

3. CURRENCIES → Standard codes
   - "€", "Euro", "EUR" → "EUR"
   - "$", "USD" → "USD"
   - "£", "GBP" → "GBP"

4. REFUND LOGIC
   - If amount is negative → type = "refund"
   - Detect refund indicators: "rimborso", "refund", "storno"

5. TEXT CLEANUP
   - Trim whitespace
   - Normalize casing where appropriate
   - Phone: standardize to "+39 XXX XXX XXXX"
   - Email: validate format, flag if invalid

Return a JSON object:
{
  "normalized_data": {
    "customers": [...],
    "policies": [...],
    "transactions": [...],
    "tickets": [...]
  },
  "normalization_issues": [
    {
      "record_type": "transaction",
      "field": "data",
      "original": "13/01/24",
      "normalized": "2024-01-13",
      "confidence": 0.95
    }
  ],
  "validation_warnings": [
    "email mario@@email.com appears invalid"
  ],
  "overall_confidence": 0.88
}"""


class NormalizationAgent:
    def __init__(self, client: RegoloClient = None):
        self.client = client or RegoloClient()
        self.model = MODEL_NORMALIZATION

    def process(self, structured_data: dict) -> dict:
        user_content = f"""Normalize this structured data.

STRUCTURED DATA:
{json.dumps(structured_data, indent=2, ensure_ascii=False)}

Return the normalized JSON output as specified."""

        response, error = self.client.call_with_retry(
            system_prompt=NORMALIZATION_SYSTEM_PROMPT,
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


def run_normalization_agent(structured_data: dict) -> dict:
    agent = NormalizationAgent()
    return agent.process(structured_data)

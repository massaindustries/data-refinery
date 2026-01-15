"""
Layout Agent - Step 3 of the pipeline.
Maps normalized data to relational database schema.
"""

import json
import re
import hashlib
from typing import Any, Optional

from config import MODEL_LAYOUT, DB_SCHEMA
from regolo_client import RegoloClient


LAYOUT_SYSTEM_PROMPT = """You are a Layout Agent mapping data to a relational database schema.

Your task is to:
1. Map normalized fields to database schema
2. Generate deterministic IDs (SHA-256 hash of unique field combination)
3. Add source references (page number, snippet)
4. Calculate per-field and per-record confidence

Database Schema:
"""

LAYOUT_SYSTEM_PROMPT += json.dumps(DB_SCHEMA, indent=2, ensure_ascii=False)

LAYOUT_SYSTEM_PROMPT += """

Rules:
- Each record must have: id, source_reference, confidence, fields
- Generate id as: hash of unique identifier fields
- source_reference = {"page": N, "snippet": "relevant text excerpt"}
- confidence = average of field confidences (0..1)
- If multiple records of same type, create array
- Flag low confidence fields (< 0.7)
- Output ONLY valid JSON - no markdown code blocks

Return JSON (no markdown, no comments):
{"customers": [...], "policies": [...], "transactions": [...], "tickets": [...], "mapping_metadata": {...}}"""


def repair_json(text: str) -> str:
    text = text.strip()
    text = re.sub(r'//.*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'/\*.*?\*/', '', text, flags=re.DOTALL)
    text = re.sub(r',\s*([}\]])', r'\1', text)
    text = re.sub(r'([{\[])\s*,', r'\1', text)
    text = re.sub(r'\n\s*"[^"]*"\s*:\s*(?=[}\]])', '', text)
    text = re.sub(r'("[^"]*")\s*:\s*"([^"]*)\s*"', r'\1: "\2"', text)
    text = re.sub(r"('[^']*')\s*:\s*'([^']*)'", r'"\1": "\2"', text)
    text = re.sub(r'^\s*```json\s*', '', text)
    text = re.sub(r'\s*```\s*$', '', text)
    text = re.sub(r'^\s*```\s*', '', text)
    text = re.sub(r'\s*```\s*$', '', text)
    return text.strip()


def extract_json_from_text(text: str) -> dict:
    text = text.strip()
    text = repair_json(text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        json_match = re.search(r'(\{[\s\S]*\})', text)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except json.JSONDecodeError:
                pass
    return {}


class LayoutAgent:
    def __init__(self, client: RegoloClient = None):
        self.client = client or RegoloClient()
        self.model = MODEL_LAYOUT

    def process(self, normalized_data: dict) -> dict:
        user_content = f"""Map this normalized data to the database schema.

NORMALIZED DATA:
{json.dumps(normalized_data, indent=2, ensure_ascii=False)}

Return ONLY valid JSON output as specified. No markdown, no explanations."""

        response, error = self.client.call_with_retry(
            system_prompt=LAYOUT_SYSTEM_PROMPT,
            user_content=user_content,
            model=self.model,
            max_tokens=4096
        )

        if error:
            return {"error": error, "success": False}

        try:
            message = response["choices"][0]["message"]
            content = message.get("content") or message.get("reasoning_content", "")
            if not content:
                return {"error": "Empty response content from API", "success": False}
            result = extract_json_from_text(content)

            if not result:
                return {"error": "Could not parse JSON from response", "success": False, "raw_response": content[:500]}

            result["success"] = True
            return result
        except Exception as e:
            return {"error": f"Parse error: {str(e)}", "success": False, "raw_response": str(response)[:500]}


def generate_deterministic_id(data: str) -> str:
    return hashlib.sha256(data.encode()).hexdigest()[:16]


def create_source_ref(page: int, snippet: str = None) -> dict:
    ref = {"page": page}
    if snippet:
        ref["snippet"] = snippet[:100]
    return ref


def run_layout_agent(normalized_data: dict) -> dict:
    agent = LayoutAgent()
    return agent.process(normalized_data)

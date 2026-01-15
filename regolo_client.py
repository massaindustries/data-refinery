"""
Regolo.ai client wrapper (OpenAI-compatible API).
"""

import json
import time
import hashlib
from typing import Any, Optional
import requests

from config import REGOLO_API_KEY, REGOLO_BASE_URL, MAX_RETRIES, INITIAL_BACKOFF


class RegoloClient:
    def __init__(self, api_key: str = None, base_url: str = None, model: str = None):
        self.api_key = api_key or REGOLO_API_KEY
        self.base_url = base_url or REGOLO_BASE_URL
        self.default_model = model

    def _make_request(
        self,
        messages: list[dict],
        model: str = None,
        tools: list[dict] = None,
        tool_choice: str = None,
        max_tokens: int = 4096
    ) -> dict:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }

        payload = {
            "model": model or self.default_model,
            "messages": messages,
            "max_tokens": max_tokens
        }

        if tools:
            payload["tools"] = tools
            payload["tool_choice"] = tool_choice or "auto"

        response = requests.post(
            f"{self.base_url}/chat/completions",
            headers=headers,
            json=payload
        )

        if response.status_code == 401:
            raise Exception(f"API Authentication failed (401). Check your API key.")

        if response.status_code == 429:
            raise Exception(f"Rate limit exceeded (429). Please wait.")

        if response.status_code >= 400:
            raise Exception(f"API error {response.status_code}: {response.text[:200]}")

        if not response.content:
            raise Exception("Empty response from API")

        response.raise_for_status()
        return response.json()

    def chat(
        self,
        system_prompt: str,
        user_content: str,
        model: str = None,
        tools: list[dict] = None,
        tool_choice: str = None
    ) -> dict:
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content}
        ]
        return self._make_request(messages, model, tools, tool_choice)

    def chat_with_history(
        self,
        messages: list[dict],
        model: str = None,
        tools: list[dict] = None,
        tool_choice: str = None
    ) -> dict:
        return self._make_request(messages, model, tools, tool_choice)

    def call_with_retry(
        self,
        system_prompt: str,
        user_content: str,
        model: str = None,
        tools: list[dict] = None,
        tool_choice: str = None,
        max_retries: int = None,
        max_tokens: int = None
    ) -> tuple[Optional[dict], Optional[str]]:
        max_retries = max_retries or MAX_RETRIES

        for attempt in range(max_retries):
            try:
                response = self.chat(system_prompt, user_content, model, tools, tool_choice)
                return response, None
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    backoff = INITIAL_BACKOFF * (2 ** attempt)
                    time.sleep(backoff)
                else:
                    return None, str(e)
            except Exception as e:
                return None, str(e)

        return None, "Max retries exceeded"


class OCRClient:
    def __init__(self, api_key: str = None, base_url: str = None):
        self.api_key = api_key or REGOLO_API_KEY
        self.base_url = base_url or REGOLO_BASE_URL

    def extract_text(self, image_b64: str) -> tuple[Optional[str], Optional[str]]:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }

        payload = {
            "model": "deepseek-ocr",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Convert the document to markdown."},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{image_b64}",
                                "format": "image/png"
                            }
                        }
                    ]
                }
            ],
            "max_tokens": 4096,
            "skip_special_tokens": False
        }

        try:
            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers=headers,
                json=payload
            )
            response.raise_for_status()
            result = response.json()
            content = result["choices"][0]["message"]["content"]
            return content, None
        except Exception as e:
            return None, str(e)


def generate_deterministic_id(data: str) -> str:
    return hashlib.sha256(data.encode()).hexdigest()[:16]


def create_source_ref(page: int, snippet: str = None) -> dict:
    ref = {"page": page}
    if snippet:
        ref["snippet"] = snippet[:100]
    return ref

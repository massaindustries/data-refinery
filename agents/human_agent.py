"""
Human-in-the-loop Agent - Step 4 of the pipeline.
Generates review reports for low confidence records.
"""

import json
from typing import Any

from config import MODEL_HUMAN_REVIEW, CONFIDENCE_THRESHOLD
from regolo_client import RegoloClient


HUMAN_REVIEW_SYSTEM_PROMPT = f"""You are a Human-in-the-Loop Review Agent.

Your task is to:
1. Identify records/fields with confidence < {CONFIDENCE_THRESHOLD}
2. Detect constraint violations (missing required fields, invalid formats)
3. Generate a detailed review report

For each issue found, provide:
- type: "low_confidence" | "missing_required" | "format_error" | "inconsistency"
- severity: "high" | "medium" | "low"
- field: the field name
- reason: why this is an issue
- evidence: page number and snippet
- suggestion: proposed fix

Return JSON:
{{
  "review_summary": {{
    "total_records": 50,
    "records_with_issues": 5,
    "issues_count": 12,
    "requires_human_review": true
  }},
  "issues": [
    {{
      "id": "issue_1",
      "type": "low_confidence",
      "severity": "high",
      "record_type": "customer",
      "record_id": "abc123",
      "field": "email",
      "confidence": 0.45,
      "reason": "Email format appears invalid",
      "evidence": {{"page": 2, "snippet": "mario@@email.com"}},
      "suggestion": "Verify correct email address with customer",
      "decision_required": true
    }}
  ],
  "auto_fixes": [
    {{
      "issue_id": "issue_2",
      "field": "telefono",
      "original": "+39 333 1234 567",
      "suggested_fix": "+39 333 1234567",
      "confidence": 0.85
    }}
  ],
  "review_recommendation": "APPROVE|REVIEW_REQUIRED|MANUAL_FIX"
}}"""


class HumanReviewAgent:
    def __init__(self, client: RegoloClient = None, threshold: float = None):
        self.client = client or RegoloClient()
        self.model = MODEL_HUMAN_REVIEW
        self.threshold = threshold or CONFIDENCE_THRESHOLD

    def process(self, db_ready_data: dict) -> dict:
        user_content = f"""Review this database-ready data and generate a human review report.

Confidence threshold: {self.threshold}

DATA:
{json.dumps(db_ready_data, indent=2, ensure_ascii=False)}

Return the review report JSON as specified."""

        response, error = self.client.call_with_retry(
            system_prompt=HUMAN_REVIEW_SYSTEM_PROMPT,
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

    def generate_markdown_report(self, review_data: dict) -> str:
        md = ["# Human Review Report\n"]

        summary = review_data.get("review_summary", {})
        md.append(f"## Summary")
        md.append(f"- Total records: {summary.get('total_records', 'N/A')}")
        md.append(f"- Records with issues: {summary.get('records_with_issues', 'N/A')}")
        md.append(f"- Issues found: {summary.get('issues_count', 'N/A')}")
        md.append(f"- Recommendation: **{review_data.get('review_recommendation', 'N/A')}**\n")

        issues = review_data.get("issues", [])
        if issues:
            md.append("## Issues Requiring Attention\n")
            for issue in issues:
                md.append(f"### {issue.get('id', 'Unknown')}")
                md.append(f"- **Type**: {issue.get('type', 'N/A')}")
                md.append(f"- **Severity**: {issue.get('severity', 'N/A')}")
                md.append(f"- **Field**: `{issue.get('field', 'N/A')}`")
                md.append(f"- **Record**: {issue.get('record_type', 'N/A')} / {issue.get('record_id', 'N/A')}")
                md.append(f"- **Confidence**: {issue.get('confidence', 'N/A')}")
                md.append(f"- **Reason**: {issue.get('reason', 'N/A')}")
                evidence = issue.get("evidence", {})
                md.append(f"- **Evidence**: Page {evidence.get('page', 'N/A')}")
                snippet = evidence.get("snippet", "")
                if snippet:
                    md.append("  ```")
                    md.append(snippet)
                    md.append("```")
                md.append(f"- **Suggestion**: {issue.get('suggestion', 'N/A')}")
                decision = "YES" if issue.get("decision_required") else "NO"
                md.append(f"- **Decision Required**: {decision}\n")

        auto_fixes = review_data.get("auto_fixes", [])
        if auto_fixes:
            md.append("## Auto-Fix Suggestions\n")
            for fix in auto_fixes:
                md.append(f"- **{fix.get('issue_id', 'Unknown')}**: `{fix.get('field', 'N/A')}`")
                md.append(f"  - Original: `{fix.get('original', 'N/A')}`")
                md.append(f"  - Suggested: `{fix.get('suggested_fix', 'N/A')}`")
                md.append(f"  - Confidence: {fix.get('confidence', 'N/A')}\n")

        return "\n".join(md)


def run_human_review_agent(db_ready_data: dict) -> dict:
    agent = HumanReviewAgent()
    return agent.process(db_ready_data)

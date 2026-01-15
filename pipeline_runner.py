"""
Pipeline runner with WebSocket event support.
"""

import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from datetime import datetime
from typing import Optional

import fitz
import base64
import requests

from config import (
    REGOLO_API_KEY, REGOLO_BASE_URL, MAX_RETRIES, INITIAL_BACKOFF, get_output_dir
)
from state_manager import PipelineState, StateManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PipelineRunner:
    def __init__(self, job_id: str, pdf_path: Path):
        self.job_id = job_id
        self.pdf_path = pdf_path
        self.output_dir = get_output_dir(str(pdf_path))
        self.output_dir.mkdir(parents=True, exist_ok=True)
        (self.output_dir / "checkpoints").mkdir(exist_ok=True)
        (self.output_dir / "final").mkdir(exist_ok=True)
        
        self.websocket = None
        self.state = PipelineState()
        self.state.source_file = str(self.output_dir / f"{pdf_path.stem}.md")
        
        self.sm = StateManager(self.output_dir)
        self.executor = ThreadPoolExecutor(max_workers=4)

    def set_websocket(self, ws):
        self.websocket = ws

    async def emit(self, event: str, data: dict):
        if self.websocket:
            try:
                await self.websocket.send_json({"event": event, "data": data})
            except Exception as e:
                logger.error(f"Error sending WebSocket message: {e}")

    async def emit_log(self, message: str):
        await self.emit("log", {"message": message, "timestamp": datetime.now().isoformat()})

    async def run(self) -> bool:
        """Run the complete pipeline."""
        try:
            # Step 1: OCR
            await self.emit("step_start", {"step": "ocr", "name": "OCR"})
            await self.emit_log("Starting OCR conversion...")
            
            md_path = self.output_dir / f"{self.pdf_path.stem}.md"
            
            # Run OCR in thread pool
            success = await self._run_ocr_async(md_path)
            
            if not success:
                await self.emit("step_error", {"step": "ocr", "message": "OCR failed"})
                await self.emit("complete", {"success": False})
                return False
            
            await self.emit("step_complete", {"step": "ocr", "file": str(md_path)})
            await self.emit_log("OCR completed successfully")

            # Load markdown content
            with open(md_path, "r", encoding="utf-8") as f:
                self.state.raw_text = f.read()

            # Step 2: Structuring
            await self.emit("step_start", {"step": "structuring", "name": "Structuring"})
            await self.emit_log("Running Structuring Agent...")
            
            # Import and run in thread pool
            from agents.structuring_agent import run_structuring_agent
            result = await self._run_agent_async(run_structuring_agent, self.state.raw_text)
            
            if not result.get("success"):
                await self._handle_agent_error("structuring", result.get("error"))
                return False
            
            self.state.structured_v0 = result
            self.sm.save_checkpoint(self.state, "structured_v0.json")
            await self.emit("step_complete", {"step": "structuring", "sections": len(result.get("sections", []))})
            await self.emit_log(f"Structuring: {len(result.get('sections', []))} sections extracted")

            # Step 3: Normalization
            await self.emit("step_start", {"step": "normalization", "name": "Normalization"})
            await self.emit_log("Running Normalization Agent...")
            
            from agents.normalization_agent import run_normalization_agent
            result = await self._run_agent_async(run_normalization_agent, self.state.structured_v0.get("extracted_fields", {}))
            
            if not result.get("success"):
                await self._handle_agent_error("normalization", result.get("error"))
                return False
            
            self.state.structured_v1 = result
            self.sm.save_checkpoint(self.state, "structured_v1_normalized.json")
            issues = len(result.get("normalization_issues", []))
            await self.emit("step_complete", {"step": "normalization", "issues": issues})
            await self.emit_log(f"Normalization: {issues} issues found")

            # Step 4: Layout
            await self.emit("step_start", {"step": "layout", "name": "Layout"})
            await self.emit_log("Running Layout Agent...")
            
            from agents.layout_agent import run_layout_agent
            result = await self._run_agent_async(run_layout_agent, self.state.structured_v1.get("normalized_data", {}))
            
            if not result.get("success"):
                await self._handle_agent_error("layout", result.get("error"))
                return False
            
            self.state.db_ready = result
            self.sm.save_checkpoint(self.state, "db_ready.json")
            self.sm.save_final(result, "db_ready.json")
            await self.emit("step_complete", {"step": "layout"})
            await self.emit_log("Layout mapping complete")

            # Step 5: Human Review
            await self.emit("step_start", {"step": "human_review", "name": "Human Review"})
            await self.emit_log("Running Human Review Agent...")
            
            from agents.human_agent import run_human_review_agent
            result = await self._run_agent_async(run_human_review_agent, self.state.db_ready)
            
            if not result.get("success"):
                await self._handle_agent_error("human_review", result.get("error"))
                return False
            
            self.state.review_report = result
            self.sm.save_checkpoint(self.state, "review_report.json")
            self.sm.save_final(result, "review_report.json")
            
            # Generate markdown report
            from agents.human_agent import HumanReviewAgent
            agent = HumanReviewAgent()
            md_content = agent.generate_markdown_report(result)
            self.sm.save_markdown_report(md_content, "review_report.md")
            
            summary = result.get("review_summary", {})
            await self.emit("step_complete", {"step": "human_review", "issues": summary.get("issues_count", 0)})
            await self.emit_log(f"Review: {summary.get('issues_count', 0)} issues found")

            # Complete
            await self.emit("complete", {"success": True, "output_dir": str(self.output_dir)})
            await self.emit_log("Pipeline completed successfully!")

            return True

        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            await self.emit("error", {"message": str(e)})
            return False

    async def _run_ocr_async(self, output_path: Path) -> bool:
        """Run OCR on PDF - async wrapper."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self._run_ocr_sync, output_path)

    def _run_ocr_sync(self, output_path: Path) -> bool:
        """Run OCR on PDF - synchronous."""
        if not self.pdf_path.exists():
            return False

        doc = fitz.open(self.pdf_path)
        all_text = []
        failed_pages = []

        for page_num in range(len(doc)):
            page_num_1 = page_num + 1

            try:
                page = doc[page_num]
                pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                image_b64 = base64.b64encode(pix.tobytes("png")).decode("utf-8")

                payload = {
                    "model": "deepseek-ocr",
                    "messages": [{"role": "user", "content": [
                        {"type": "text", "text": "Convert to markdown."},
                        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}", "format": "image/png"}}
                    ]}],
                    "max_tokens": 4096
                }

                for attempt in range(MAX_RETRIES):
                    try:
                        resp = requests.post(f"{REGOLO_BASE_URL}/chat/completions",
                            headers={"Authorization": f"Bearer {REGOLO_API_KEY}"}, json=payload)
                        resp.raise_for_status()
                        content = resp.json()["choices"][0]["message"]["content"]
                        all_text.append(f"\n\n--- Page {page_num_1} ---\n\n{content}")
                        break
                    except Exception as e:
                        if attempt < MAX_RETRIES - 1:
                            pass  # Retry silently
                        else:
                            failed_pages.append(page_num_1)
            except Exception as e:
                failed_pages.append(page_num_1)

        doc.close()

        with open(output_path, "w", encoding="utf-8") as f:
            f.write("".join(all_text))

        if failed_pages:
            logger.warning(f"OCR failed pages: {failed_pages}")

        return len(failed_pages) == 0

    async def _run_agent_async(self, agent_func, input_data):
        """Run an agent function in a thread pool."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, lambda: agent_func(input_data))

    async def _handle_agent_error(self, agent_name: str, error: str):
        """Handle agent error with retry messaging."""
        for attempt in range(1, MAX_RETRIES + 1):
            if attempt < MAX_RETRIES:
                backoff = INITIAL_BACKOFF * (2 ** (attempt - 1))
                await self.emit_log(f"Agent '{agent_name}' failed. Retrying in {backoff}s...")
                await asyncio.sleep(backoff)
            else:
                await self.emit("step_error", {"step": agent_name, "message": f"Failed after {MAX_RETRIES} retries: {error}"})
                await self.emit("complete", {"success": False, "error": error})


async def run_pipeline_sync(job_id: str, pdf_path: Path, ws) -> bool:
    """Synchronous wrapper for running pipeline."""
    runner = PipelineRunner(job_id, pdf_path)
    runner.set_websocket(ws)
    return await runner.run()

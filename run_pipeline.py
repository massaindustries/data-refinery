#!/usr/bin/env python3
"""
Multi-agent data cleaning pipeline with OCR.
"""

import sys
import argparse
import shutil
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.resolve()))

import fitz
import base64
import requests
from rich.console import Console
from rich.theme import Theme
from rich.panel import Panel
from rich.text import Text
from rich.table import Table
from rich.box import ROUNDED
from rich import print as rprint

from config import (
    SOURCE_PDF, BASE_DIR, REGOLO_API_KEY, REGOLO_BASE_URL,
    MAX_RETRIES, INITIAL_BACKOFF, get_output_dir
)
from state_manager import StateManager
from orchestrator import Orchestrator

console = Console(theme=Theme({
    "info": "cyan",
    "success": "green",
    "warning": "yellow",
    "error": "red",
    "agent": "magenta",
}))


def log(msg: str, style: str = "info"):
    rprint(f"[{style}][{datetime.now().strftime('%H:%M:%S')}][/] {msg}")


def log_step(agent_name: str, status: str, details: str = "", style: str = "info"):
    panel = Panel(
        Text(f"{status}\n{details}", justify="left"),
        title=f"[agent]{agent_name}[/]",
        box=ROUNDED,
        style="info"
    )
    console.print(panel)


def log_success(msg: str):
    rprint(f"[success]✓[/] {msg}")


def log_error(msg: str):
    rprint(f"[error]✗[/] {msg}")


def log_section(title: str):
    rprint(f"\n[info]━━━ {title} ━━━[/]\n")


def pdf_to_markdown(pdf_path: Path, output_path: Path, api_key: str) -> bool:
    if not pdf_path.exists():
        log(f"PDF not found: {pdf_path}", "error")
        return False

    log(f"Starting OCR conversion of [info]{pdf_path.name}[/]")
    doc = fitz.open(pdf_path)
    all_text = []
    failed = []

    for page_num in range(len(doc)):
        page_num_1 = page_num + 1
        log(f"Processing page {page_num_1}/{len(doc)}...", "info")

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
                        headers={"Authorization": f"Bearer {api_key}"}, json=payload)
                    resp.raise_for_status()
                    content = resp.json()["choices"][0]["message"]["content"]
                    all_text.append(f"\n\n--- Page {page_num_1} ---\n\n{content}")
                    break
                except Exception as e:
                    if attempt < MAX_RETRIES - 1:
                        log(f"Retry {attempt+1}/{MAX_RETRIES-1}...", "warning")
                    else:
                        failed.append(page_num_1)
                        log(f"FAILED page {page_num_1}", "error")

        except Exception as e:
            failed.append(page_num_1)
            log(f"ERROR page {page_num_1}: {e}", "error")

    total_pages = len(doc)
    doc.close()

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("".join(all_text))

    if failed:
        log(f"Completed - {total_pages - len(failed)}/{total_pages} pages OK, {len(failed)} failed", "warning")
        for p in failed:
            log(f"  Failed page: {p}", "error")
    else:
        log(f"Completed - {total_pages}/{total_pages} pages OK", "success")

    return len(failed) == 0


def show_summary(state):
    table = Table(title="Pipeline Summary", box=ROUNDED)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="magenta")

    table.add_row("Source", state.source_file)
    table.add_row("Output", str(state.output_dir))
    table.add_row("Agents", f"{len(state.completed_agents)}/4 completed")
    table.add_row("Errors", str(len(state.errors)))

    if state.structured_v0:
        table.add_row("Sections", str(len(state.structured_v0.get('sections', []))))

    if state.structured_v1:
        issues = len(state.structured_v1.get('normalization_issues', []))
        table.add_row("Normalization Issues", str(issues))

    if state.db_ready:
        meta = state.db_ready.get('mapping_metadata', {})
        table.add_row("Records", str(meta.get('records_processed', 0)))

    console.print(table)

    rprint(f"\n[info]📁 Checkpoints:[/] {state.output_dir / 'checkpoints'}")
    rprint(f"[info]📄 Final:[/] {state.output_dir / 'final'}")


def reset_pipeline(output_dir: Path):
    if output_dir.exists():
        shutil.rmtree(output_dir)
    log("Pipeline cleared", "warning")


def main():
    parser = argparse.ArgumentParser(description="Data cleaning pipeline")
    parser.add_argument("input", nargs="?", type=str, default=str(SOURCE_PDF), help="Input PDF or MD file")
    parser.add_argument("--skip-ocr", action="store_true", help="Skip OCR")
    parser.add_argument("--reset", action="store_true", help="Reset pipeline")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = get_output_dir(str(input_path))

    log_section("PIPELINE START")
    log(f"Input: [info]{input_path}[/]")
    log(f"Output: [info]{output_dir}[/]")

    if args.reset:
        reset_pipeline(output_dir)
        return 0

    md_path = output_dir / f"{input_path.stem}.md"

    if not args.skip_ocr:
        if input_path.suffix.lower() == ".pdf":
            pdf_to_markdown(input_path, md_path, REGOLO_API_KEY)
        else:
            log(f"Unsupported file format: {input_path}", "error")
            return 1

    if not md_path.exists():
        log(f"{md_path} not found", "error")
        return 1

    log_section("AGENTS PIPELINE")

    orchestrator = Orchestrator(output_dir)
    if not orchestrator.initialize(str(md_path)):
        log("Init failed", "error")
        return 1

    if orchestrator.run_pipeline():
        log_section("PIPELINE COMPLETE")
        show_summary(orchestrator.state)
        rprint(f"\n[success]✓ Pipeline completed successfully[/]")
        return 0
    else:
        log_section("PIPELINE FAILED")
        rprint(f"\n[error]✗ Pipeline failed[/]")
        return 1


if __name__ == "__main__":
    sys.exit(main())

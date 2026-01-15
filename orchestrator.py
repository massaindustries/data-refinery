"""
Orchestrator for the multi-agent data cleaning pipeline.
Implements handoffs between agents.
"""

import sys
from pathlib import Path
from typing import Optional

from state_manager import PipelineState, StateManager
from config import MAX_RETRIES
from agents.structuring_agent import run_structuring_agent
from agents.normalization_agent import run_normalization_agent
from agents.layout_agent import run_layout_agent
from agents.human_agent import run_human_review_agent


AGENT_ORDER = ["structuring", "normalization", "layout", "human_review"]

CHECKPOINT_FILES = {
    "raw": "raw_text.json",
    "structured_v0": "structured_v0.json",
    "normalized": "structured_v1_normalized.json",
    "db_ready": "db_ready.json",
    "review": "review_report.json"
}


class Orchestrator:
    def __init__(self, output_dir: Path = None):
        self.output_dir = output_dir
        self.state_manager = StateManager(output_dir)
        self.state = PipelineState()

    def initialize(self, source_file: str) -> bool:
        self.state.source_file = source_file
        self.state.current_agent = "init"
        self.state.retry_count = 0
        self.state.errors = []
        self.state.completed_agents = []

        try:
            with open(source_file, "r", encoding="utf-8") as f:
                self.state.raw_text = f.read()
        except Exception as e:
            self.state.errors.append(f"Failed to read source file: {e}")
            return False

        self.state.current_agent = "structuring"
        self._save_checkpoint(CHECKPOINT_FILES["raw"])
        return True

    def run_pipeline(self) -> bool:
        print("=" * 60)
        print("MULTI-AGENT DATA CLEANING PIPELINE")
        print("=" * 60)

        for agent_name in AGENT_ORDER:
            self.state.current_agent = agent_name
            self.state.retry_count = 0

            success = self._execute_agent(agent_name)
            if not success:
                print(f"\nAgent '{agent_name}' failed after {MAX_RETRIES} retries")
                self.state.errors.append(f"Agent '{agent_name}' failed")
                self._save_checkpoint(f"failed_{agent_name}.json")
                return False

            self.state.completed_agents.append(agent_name)

        self._save_final_outputs()

        print("\n" + "=" * 60)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 60)
        self._print_summary()
        return True

    def _execute_agent(self, agent_name: str) -> bool:
        max_retries = MAX_RETRIES

        for attempt in range(max_retries):
            self.state.retry_count = attempt + 1
            print(f"\n[AGENT: {agent_name}] Attempt {attempt + 1}/{max_retries}")

            try:
                if agent_name == "structuring":
                    result = run_structuring_agent(self.state.raw_text)
                    if result.get("success"):
                        self.state.structured_v0 = result
                        self._save_checkpoint(CHECKPOINT_FILES["structured_v0"])
                        print(f"  -> Structured V0: {len(result.get('sections', []))} sections")
                        return True
                    else:
                        print(f"  -> Error: {result.get('error')}")

                elif agent_name == "normalization":
                    result = run_normalization_agent(self.state.structured_v0.get("extracted_fields", {}))
                    if result.get("success"):
                        self.state.structured_v1 = result
                        self._save_checkpoint(CHECKPOINT_FILES["normalized"])
                        print(f"  -> Normalized: {len(result.get('normalization_issues', []))} issues")
                        return True
                    else:
                        print(f"  -> Error: {result.get('error')}")

                elif agent_name == "layout":
                    result = run_layout_agent(self.state.structured_v1.get("normalized_data", {}))
                    if result.get("success"):
                        self.state.db_ready = result
                        self._save_checkpoint(CHECKPOINT_FILES["db_ready"])
                        metadata = result.get("mapping_metadata", {})
                        print(f"  -> DB Ready: {metadata.get('records_processed', 0)} records")
                        return True
                    else:
                        print(f"  -> Error: {result.get('error')}")

                elif agent_name == "human_review":
                    result = run_human_review_agent(self.state.db_ready)
                    if result.get("success"):
                        self.state.review_report = result
                        self._save_checkpoint(CHECKPOINT_FILES["review"])
                        self._generate_markdown_report(result)
                        summary = result.get("review_summary", {})
                        print(f"  -> Review: {summary.get('issues_count', 0)} issues found")
                        return True
                    else:
                        print(f"  -> Error: {result.get('error')}")

            except Exception as e:
                print(f"  -> Exception: {e}")
                self.state.errors.append(f"{agent_name}: {str(e)}")

            if attempt < max_retries - 1:
                backoff = 2 * (2 ** attempt)
                print(f"  -> Retrying in {backoff}s...")
        return False

    def _save_checkpoint(self, filename: str):
        path = self.state_manager.save_checkpoint(self.state, filename)
        if path:
            print(f"  [Checkpoint saved: {path}]")

    def _save_final_outputs(self):
        self.state_manager.save_final(self.state, "pipeline_state.json")
        self.state_manager.save_final(self.state.db_ready, "db_ready.json")
        self.state_manager.save_final(self.state.review_report, "review_report.json")
        print(f"  [Final outputs saved to {self.output_dir / 'final'}]")

    def _generate_markdown_report(self, review_data: dict):
        from agents.human_agent import HumanReviewAgent
        agent = HumanReviewAgent()
        md_content = agent.generate_markdown_report(review_data)
        path = self.state_manager.save_markdown_report(md_content, "review_report.md")
        if path:
            print(f"  [Report saved: {path}]")

    def _print_summary(self):
        print(f"\nPipeline Summary:")
        print(f"  Source: {self.state.source_file}")
        print(f"  Output: {self.output_dir}")
        print(f"  Agents: {len(self.state.completed_agents)}/{len(AGENT_ORDER)} completed")
        print(f"  Errors: {len(self.state.errors)}")

        if self.state.structured_v0:
            print(f"  Structured V0: {len(self.state.structured_v0.get('sections', []))} sections")

        if self.state.structured_v1:
            issues = len(self.state.structured_v1.get("normalization_issues", []))
            print(f"  Normalization: {issues} issues")

        if self.state.db_ready:
            meta = self.state.db_ready.get("mapping_metadata", {})
            print(f"  Records: {meta.get('records_processed', 0)}")

        print(f"\n  Checkpoints: {self.output_dir / 'checkpoints'}")
        print(f"  Final: {self.output_dir / 'final'}")

    def get_state(self) -> PipelineState:
        return self.state

    def load_from_checkpoint(self, checkpoint_file: str) -> bool:
        state = self.state_manager.load_checkpoint(checkpoint_file)
        if state:
            self.state = state
            return True
        return False

"""
State manager for checkpoint loading and saving.
"""

import json
from pathlib import Path
from typing import Optional, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime


@dataclass
class PipelineState:
    source_file: str = ""
    current_agent: str = "init"
    checkpoint_file: Optional[str] = None
    retry_count: int = 0
    errors: list = field(default_factory=list)
    completed_agents: list = field(default_factory=list)

    raw_text: str = ""
    structured_v0: dict = field(default_factory=dict)
    structured_v1: dict = field(default_factory=dict)
    db_ready: dict = field(default_factory=dict)
    review_report: dict = field(default_factory=dict)

    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "PipelineState":
        state = cls()
        for key, value in data.items():
            if hasattr(state, key):
                setattr(state, key, value)
        return state


class StateManager:
    def __init__(self, output_dir: Optional[Path] = None):
        self.output_dir = output_dir
        self.checkpoint_dir = output_dir / "checkpoints" if output_dir else None
        self.final_dir = output_dir / "final" if output_dir else None

    def save_checkpoint(self, state: PipelineState, filename: str) -> Optional[Path]:
        if not self.checkpoint_dir:
            return None
        state.updated_at = datetime.now().isoformat()
        checkpoint_path = self.checkpoint_dir / filename
        with open(checkpoint_path, "w", encoding="utf-8") as f:
            json.dump(state.to_dict(), f, indent=2, ensure_ascii=False)
        return checkpoint_path

    def save_final(self, state_or_dict: Union[PipelineState, dict], filename: str) -> Optional[Path]:
        if not self.final_dir:
            return None
        final_path = self.final_dir / filename
        if isinstance(state_or_dict, PipelineState):
            data = state_or_dict.to_dict()
        else:
            data = state_or_dict
        with open(final_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return final_path

    def load_checkpoint(self, filename: str) -> Optional[PipelineState]:
        if not self.checkpoint_dir:
            return None
        checkpoint_path = self.checkpoint_dir / filename
        if checkpoint_path.exists():
            with open(checkpoint_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return PipelineState.from_dict(data)
        return None

    def save_markdown_report(self, content: str, filename: str = "review_report.md") -> Optional[Path]:
        if not self.final_dir:
            return None
        report_path = self.final_dir / filename
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(content)
        return report_path

    def get_checkpoint_path(self, filename: str) -> Optional[Path]:
        return self.checkpoint_dir / filename if self.checkpoint_dir else None

    def get_final_path(self, filename: str) -> Optional[Path]:
        return self.final_dir / filename if self.final_dir else None

    def list_checkpoints(self) -> list[Path]:
        if not self.checkpoint_dir:
            return []
        return list(self.checkpoint_dir.glob("*.json"))

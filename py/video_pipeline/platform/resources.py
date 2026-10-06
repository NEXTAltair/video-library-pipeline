"""Public defaults with optional local-only overrides."""
from pathlib import Path


def default_program_aliases(py_root: Path) -> Path:
    rules = py_root.parent / "rules"
    local = rules / "program_aliases.yaml"
    return local if local.is_file() else rules / "program_aliases.example.yaml"

"""The checkout reserves .venv so a plain `uv run` cannot create one inside the plugin."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_checkout_venv_is_reserved_regular_file():
    guard = ROOT / ".venv"
    assert guard.is_file() and not guard.is_symlink()

import os
from pathlib import Path
import subprocess

WRAPPER = Path(__file__).resolve().parents[2] / "scripts" / "uv-project"


def test_wrapper_uses_external_state_and_passes_arguments(tmp_path):
    fake_uv = tmp_path / "uv"
    fake_uv.write_text('#!/bin/sh\nprintf "%s\\n" "$UV_PROJECT_ENVIRONMENT" "$@"\n')
    fake_uv.chmod(0o755)
    env = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}",
           "OPENCLAW_STATE_DIR": str(tmp_path / "state")}
    env.pop("UV_PROJECT_ENVIRONMENT", None)
    env.pop("OPENCLAW_VIDEO_PIPELINE_VENV", None)
    result = subprocess.run([str(WRAPPER), "run", "python", "-V"], cwd=tmp_path,
                            env=env, text=True, capture_output=True, check=True)
    assert result.stdout.splitlines() == [str(tmp_path / "state/plugin-runtime/video-library-pipeline/.venv"), "run", "python", "-V"]
    assert not (tmp_path / ".venv").exists()

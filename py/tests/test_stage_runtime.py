import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]


def test_stage_runtime_isolated_from_checkout(tmp_path):
    env = {**os.environ, 'OPENCLAW_VIDEO_PIPELINE_RELEASES': str(tmp_path / 'releases')}
    result = subprocess.run([str(ROOT / 'scripts/stage-runtime')], cwd=tmp_path,
                            env=env, text=True, capture_output=True, check=True)
    release = Path(result.stdout.strip())
    assert release.parent == tmp_path / 'releases'
    assert (release / 'dist/index.js').is_file()
    assert (release / 'openclaw.plugin.json').is_file()
    assert (release / 'uv.lock').is_file()
    assert not (release / '.venv').exists()
    assert not list(release.parent.glob('.stage.*'))

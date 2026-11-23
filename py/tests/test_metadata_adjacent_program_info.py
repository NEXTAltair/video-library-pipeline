from pathlib import Path
import errno

import pytest

import run_metadata_batches_promptv1 as metadata


def fail_sidecar_stat(monkeypatch, err, message):
    original = Path.stat

    def stat(path, **kwargs):
        if path.name.endswith(".mp4.program.txt"):
            raise OSError(err, message, path)
        return original(path, **kwargs)

    monkeypatch.setattr(Path, "stat", stat)


def test_paired_mp4_program_txt_is_parsed(tmp_path: Path):
    video = tmp_path / "Show_2026_11_01_21_00.mp4"
    video.touch()
    sidecar = Path(str(video) + ".program.txt")
    sidecar.write_text("2026/11/01(日) 21:00～21:30\nNHK総合\nローカル番組\n説明\n", encoding="utf-8")

    parsed = metadata._paired_program_info(str(video))

    assert parsed is not None
    assert parsed["official_title"] == "ローカル番組"
    assert metadata._paired_program_info(str(tmp_path / "other.mp4")) is None


def test_candidate_hints_put_exact_sidecar_ahead_of_epg_cache(tmp_path: Path):
    video = tmp_path / "Show_2026_11_01_21_00.mp4"
    video.touch()
    Path(str(video) + ".program.txt").write_text("2026/11/01(日) 21:00～21:30\nNHK総合\n隣接番組\n説明\n", encoding="utf-8")

    class Cache:
        def candidates(self, match_key, datetime_key):
            return [{"official_title": "別番組"}]

    paired_by_path = metadata._paired_program_info_batch([str(video)])
    row = metadata._build_llm_input_rows([{"path": str(video), "name": video.name}], Cache(), paired_by_path)[0]
    assert row["epg_hint_titles"][:2] == ["隣接番組", "別番組"]


def test_paired_program_info_localizes_windows_path_and_uses_exact_sidecar(tmp_path: Path, monkeypatch):
    video = tmp_path / "very-long-recording-name.mp4"
    video.touch()
    sidecar = Path(str(video) + ".program.txt")
    sidecar.write_text("2026/11/01(日) 21:00～21:30\nNHK総合\nWindows sidecar\n説明\n", encoding="utf-8")
    windows = r"B:\VideoLibrary\very-long-recording-name.mp4"
    monkeypatch.setattr(metadata, "windows_to_wsl_path", lambda value: str(video) if value == windows else value)

    parsed = metadata._paired_program_info(windows)

    assert parsed["official_title"] == "Windows sidecar"


@pytest.mark.parametrize("err", [errno.ENAMETOOLONG, errno.EIO])
def test_paired_program_info_uses_native_batch_for_wsl_errors(monkeypatch, err):
    existing = "B:\\Recorded\\" + "番" * 100 + "'[1].mp4"
    missing = "/mnt/b/Recorded/" + "組" * 100 + ".mp4"
    calls = []

    def native(paths):
        calls.append(paths)
        return ["2026/11/01(日) 21:00～21:30\nNHK総合\n隣接番組\n".encode("utf-8-sig"), None]

    fail_sidecar_stat(monkeypatch, err, "WSL cannot read component")
    monkeypatch.setattr(metadata, "read_windows_program_info", native)

    result = metadata._paired_program_info_batch([existing, missing, existing, "other.ts"])

    assert result[existing]["official_title"] == "隣接番組"
    assert result[missing] is None
    assert result["other.ts"] is None
    assert calls == [[
        Path(metadata.windows_to_wsl_path(existing) + ".program.txt"),
        Path(missing + ".program.txt"),
    ]]


def test_paired_program_info_surfaces_windows_read_error(monkeypatch):
    def native_error(paths):
        raise PermissionError("access denied", paths[0])

    fail_sidecar_stat(monkeypatch, errno.ENAMETOOLONG, "WSL cannot read component")
    monkeypatch.setattr(metadata, "read_windows_program_info", native_error)

    with pytest.raises(PermissionError, match="access denied"):
        metadata._paired_program_info(r"B:\Recorded\recording.mp4")


@pytest.mark.parametrize("err", [errno.EIO, errno.EACCES, errno.ENAMETOOLONG])
def test_paired_program_info_does_not_hide_local_access_errors(tmp_path, monkeypatch, err):
    fail_sidecar_stat(monkeypatch, err, "local read failed")
    with pytest.raises(OSError, match="local read failed"):
        metadata._paired_program_info(str(tmp_path / "recording.mp4"))


def test_native_reader_preserves_literal_paths_and_distinguishes_missing(monkeypatch):
    import json
    from subprocess import CompletedProcess
    from video_pipeline.platform import native_program_info

    def run(args, *, input, **kwargs):
        assert args[:4] == ["pwsh.exe", "-NoProfile", "-NonInteractive", "-EncodedCommand"]
        assert json.loads(input) == [r"B:\'番組[1].mp4.program.txt", r"E:\組.mp4.program.txt"]
        return CompletedProcess(args, 0, '[{"status":"read","data":"77u/AA=="},{"status":"missing"}]', "")

    monkeypatch.setattr(native_program_info, "_pwsh", lambda: "pwsh.exe")
    monkeypatch.setattr(native_program_info.subprocess, "run", run)

    assert native_program_info.read_windows_program_info([
        Path("/mnt/b/'番組[1].mp4.program.txt"),
        Path("/mnt/e/組.mp4.program.txt"),
    ]) == [b"\xef\xbb\xbf\x00", None]


@pytest.mark.parametrize(
    ("returncode", "response", "exception", "message"),
    [
        (0, '[{"status":"error","message":"Access denied"}]', OSError, "Access denied"),
        (0, '[]', ValueError, "incomplete batch"),
        (0, '[{"exists":false}]', ValueError, "Invalid Windows program info response"),
        (1, '', OSError, "reader failed"),
    ],
)
def test_native_reader_does_not_report_failed_probe_as_missing(
    monkeypatch, returncode, response, exception, message
):
    from subprocess import CompletedProcess
    from video_pipeline.platform import native_program_info

    monkeypatch.setattr(native_program_info, "_pwsh", lambda: "pwsh.exe")
    monkeypatch.setattr(native_program_info.subprocess, "run", lambda *args, **kw: CompletedProcess(
        args, returncode, response, "failure" if returncode else ""
    ))

    with pytest.raises(exception, match=message):
        native_program_info.read_windows_program_info([Path("/mnt/b/recording.mp4.program.txt")])

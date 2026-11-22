from pathlib import Path
import errno

import run_metadata_batches_promptv1 as metadata


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

    row = metadata._build_llm_input_rows([{"path": str(video), "name": video.name}], Cache())[0]
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


def test_paired_program_info_ignores_wsl_eio(tmp_path: Path, monkeypatch):
    video = tmp_path / "recording.mp4"
    video.touch()
    monkeypatch.setattr(metadata.Path, "is_file", lambda _self: (_ for _ in ()).throw(OSError(errno.EIO, "I/O error")))
    assert metadata._paired_program_info(str(video)) is None

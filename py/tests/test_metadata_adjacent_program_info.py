from pathlib import Path

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

import re
import pytest
from path_placement_rules import safe_dir_name, build_expected_dest_path
from video_pipeline.domain.work_title_normalizer import resolve_work_title
from video_pipeline.domain.series_name_extractor import extract_series_name

@pytest.mark.parametrize('raw,expected',[
 ('YOUは何しに日本へ?', 'YOUは何しに日本へ？'),
 ('牙狼<GARO>', '牙狼＜GARO＞'),
 ('機動戦士ガンダムユニコーン RE:0096','機動戦士ガンダムユニコーン RE：0096'),
 ('仮面ライダーガッチャード"','仮面ライダーガッチャード'),
 (' _＿ Title＿_ . ','Title'),
 ('A/B\\C|D*E','A／B＼C｜D＊E'),
 ('GARO_VERSUS_ROAD','GARO_VERSUS_ROAD'),
 ('___＿','UNKNOWN'),
 ('「番組」','「番組」'),
])
def test_readable_windows_folder(raw,expected):
 assert safe_dir_name(raw)==expected
 assert safe_dir_name(expected)==expected
 assert not re.search(r'[<>:"/\\|?*]',safe_dir_name(raw))

def test_title_folder_shared_legacy_builder():
 md={'program_title':'YOUは何しに日本へ?','air_date':'2026-11-15','needs_review':False}
 dst,error=build_expected_dest_path(r'B:\VideoLibrary',r'B:\未視聴\record.mp4',md)
 assert error is None
 assert dst==r'B:\VideoLibrary\YOUは何しに日本へ？\2026\11\record.mp4'

def test_long_folder_hash_suffix_is_stable():
 a=safe_dir_name('題'*100+'?')
 assert len(a)<=60 and re.search(r'_[0-9a-f]{8}$',a)
 assert safe_dir_name(a)==a

@pytest.mark.parametrize('raw,expected',[
 ('仮面ライダーガッチャード"','仮面ライダーガッチャード'),
 ("Title'",'Title'),('Title」','Title'),('【Title','Title'),('Title】）','Title'),
 ('「Title」','「Title」'),('【Title】','【Title】'),("NNNドキュメント'26","NNNドキュメント'26"),
 ("Rock 'n' Roll","Rock 'n' Roll"),('牙狼<GARO>','牙狼<GARO>'),
])
def test_canonical_outputs_trim_only_unmatched_edges(raw,expected):
 assert resolve_work_title('Recording',{'Recording':raw})==expected
 assert resolve_work_title('Recording',dictionary_match=lambda _:raw)==expected

def test_ingest_and_extract_share_quote_cleanup():
 assert extract_series_name('仮面ライダーガッチャード"',_alias_map={})=='仮面ライダーガッチャード'
 assert resolve_work_title('Recording',regex_rules=[(re.compile('Recording'),'仮面ライダーガッチャード"','base')])=='仮面ライダーガッチャード'

def test_existing_reviewed_title_quote_is_repaired_before_exact_match():
 from title_resolution import CanonicalTitleSources,suggest_canonical_title
 raw='仮面ライダーガッチャード"'
 src=CanonicalTitleSources((raw,),(),frozenset((raw.casefold(),)),())
 assert suggest_canonical_title(raw,src,min_extra_chars=3)==('仮面ライダーガッチャード','edge_delimiters')

def test_edge_cleanup_does_not_skip_canonical_or_subtitle_resolution():
 from title_resolution import CanonicalTitleSources,suggest_canonical_title
 src=CanonicalTitleSources(('Example Show',),(),frozenset(('example show',)),())
 assert suggest_canonical_title('Example Show▽Episode"',src,min_extra_chars=3)==('Example Show','human_reviewed')
 assert suggest_canonical_title('Example Show"',src,min_extra_chars=3)==('Example Show','edge_delimiters')
 empty=CanonicalTitleSources((),(),frozenset(),())
 assert suggest_canonical_title('Example Show▽Episode"',empty,min_extra_chars=3)==('Example Show','separator_split')

@pytest.mark.parametrize('extra,reason',[
 ({'air_date':'not-a-date'},'invalid_air_date_format'),
 ({'confidence':'not-a-number'},'invalid_confidence'),
 ({'needs_review':True,'needs_review_reason':'epg_identity_unresolved'},'epg_identity_unresolved'),
])
def test_edge_only_fix_preserves_unrelated_review_in_apply(tmp_path,monkeypatch,extra,reason):
 import json,sys
 import apply_llm_extract_output as apply
 from mediaops_schema import connect_db,create_schema_if_needed
 db=tmp_path/'db.sqlite';con=connect_db(str(db));create_schema_if_needed(con)
 con.execute("insert into paths(path_id,path,created_at,updated_at) values('pid','record.mp4','now','now')")
 con.commit();con.close()
 rec={'path_id':'pid','program_title':'Example Show"','air_date':'2026-11-15','needs_review':False,'confidence':0.9}
 rec.update(extra);inp=tmp_path/'input.jsonl';inp.write_text(json.dumps(rec)+'\n')
 monkeypatch.setattr(sys,'argv',['apply','--db',str(db),'--in',str(inp)])
 assert apply.main()==0
 con=connect_db(str(db));row=con.execute("select * from path_metadata where path_id='pid'").fetchone()
 assert row['program_title']=='Example Show'
 assert row['needs_review']==1
 assert reason in json.loads(row['data_json'])['needs_review_reason']
 con.close()

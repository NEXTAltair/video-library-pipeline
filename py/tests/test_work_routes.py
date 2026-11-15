from pathlib import PureWindowsPath
import pytest
from video_pipeline.domain.path_placement_rules import DriveRoute, DriveRoutes, build_routed_dest_path, load_drive_routes
from video_pipeline.domain.movie_grouping import MovieGrouping

WORK='牙狼<GARO> -魔戒ノ花-'
ROOT=r'N:\シリーズ_牙狼'


def policy():
 return [{'works':[WORK,'劇場版 牙狼<GARO>-GOLD STORM-翔','牙狼<Garo> -Vanishing Line-'],
          'dest_root':ROOT,'layout':'by_title'}]


def routes(work_routes=None):
 return DriveRoutes(r'B:\VideoLibrary','by_program_year_month',[
  DriveRoute({'genre':'tokusatsu','dest_root':'N:\\','layout':'by_title'}),
  DriveRoute({'genre':'anime','dest_root':r'D:\Anime','layout':'by_title'}),
  DriveRoute({'genre':'movie','dest_root':'L:\\','layout':'by_syllabary'}),
 ],work_routes=policy() if work_routes is None else work_routes)


@pytest.mark.parametrize('title,genre',[(WORK,'特撮'),('劇場版 牙狼<GARO>-GOLD STORM-翔','映画'),('牙狼<Garo> -Vanishing Line-','アニメ')])
def test_work_override_changes_storage_only(title,genre):
 r=routes();md={'program_title':title,'air_date':'2026-01-01','needs_review':False,'genre':genre}
 before=dict(md);dst,route,error=build_routed_dest_path(r,r'B:\Old\recording.mp4',md)
 assert error is None and dst.startswith(ROOT+'\\')
 assert PureWindowsPath(dst).parent.name in ['牙狼＜GARO＞ -魔戒ノ花-','劇場版 牙狼＜GARO＞-GOLD STORM-翔','牙狼＜Garo＞ -Vanishing Line-']
 assert route=={'特撮':'tokusatsu','映画':'movie','アニメ':'anime'}[genre]
 assert md==before


def test_existing_member_never_leaves_series_root():
 md={'program_title':WORK,'air_date':'2026-01-01','needs_review':False,'genre':'特撮'}
 src=ROOT+r'\牙狼＜GARO＞ -魔戒ノ花-\recording.mp4'
 assert build_routed_dest_path(routes(),src,md)==(src,'tokusatsu',None)


def test_declared_work_overrides_movie_grouping():
 r=routes();r.movie_grouping=MovieGrouping({'frames':['金曜ロードショー'],'default_dest_root':'L:\\','series':[]})
 md={'program_title':'劇場版 牙狼<GARO>-GOLD STORM-翔','air_date':'2026-01-01','needs_review':False,'genre':'映画'}
 assert build_routed_dest_path(r,r'B:\Old\金曜ロードショー「牙狼」.mp4',md)[0].startswith(ROOT+'\\')


@pytest.mark.parametrize('title',['牙狼紹介番組','無関係番組','Unknown'])
def test_unknown_member_is_held_instead_of_moved_out(title):
 md={'program_title':title,'air_date':'2026-01-01','needs_review':False,'genre':'特撮'}
 assert build_routed_dest_path(routes(),ROOT+r'\old\recording.mp4',md)==(None,'tokusatsu','series_work_unmapped')


def test_prefix_and_description_mentions_do_not_match():
 md={'program_title':WORK+'の魅力','air_date':'2026-01-01','needs_review':False,'genre':'特撮'}
 dst,_,error=build_routed_dest_path(routes(),r'B:\Old\recording.mp4',md)
 assert error is None and not dst.startswith(ROOT+'\\')
 md['program_title']='紹介番組'
 dst,_,error=build_routed_dest_path(routes(),r'B:\Old\牙狼<GARO> -魔戒ノ花-を紹介.mp4',md)
 assert error is None and not dst.startswith(ROOT+'\\')


def test_sibling_root_prefix_does_not_count_as_membership():
 md={'program_title':'Unknown','air_date':'2026-01-01','needs_review':False,'genre':'特撮'}
 assert build_routed_dest_path(routes(),ROOT+r'別\recording.mp4',md)[2] is None


@pytest.mark.parametrize('bad',[
 'invalid',{},[None],[{}],[{'works':'Title','dest_root':ROOT}],
 [{'works':[''],'dest_root':ROOT}],[{'works':[1],'dest_root':ROOT}],
 [{'works':['Title'],'dest_root':'N:'}],[{'works':['Title'],'dest_root':'relative'}],
 [{'works':['Title'],'dest_root':'/tmp/dir'}],[{'works':['Title'],'dest_root':ROOT,'layout':'flat'}],
 [{'works':['Title','Ｔｉｔｌｅ'],'dest_root':ROOT}],
 [{'works':['Title'],'dest_root':ROOT},{'works':['Title'],'dest_root':r'D:\Other'}],
])
def test_invalid_or_duplicate_policy_rejected(bad):
 with pytest.raises(ValueError):routes(bad)


def test_roots_are_discovered_and_plain_routes_stay_compatible(tmp_path):
 import yaml
 from video_pipeline.domain.routing_inventory import default_relocate_roots
 from dedup_cli import configured_roots
 p=tmp_path/'routes.yaml';p.write_text(yaml.safe_dump({'default_dest':r'B:\VideoLibrary','routes':[],
 'work_routes':policy()},allow_unicode=True))
 loaded=load_drive_routes(p)
 assert ROOT in default_relocate_roots(r'B:\Source',r'B:\VideoLibrary',r'J:\TVFile',str(p))
 assert '/mnt/n/シリーズ_牙狼' in configured_roots(r'B:\Source',r'B:\VideoLibrary',str(p))
 assert loaded.work_routes
 old=DriveRoutes(r'B:\VideoLibrary','by_title',[])
 md={'program_title':WORK,'air_date':'2026-01-01','needs_review':False}
 assert build_routed_dest_path(old,r'B:\old\record.mp4',md)[0]==r'B:\VideoLibrary\牙狼＜GARO＞ -魔戒ノ花-\record.mp4'


def test_exact_matching_normalizes_width_case_not_work_identity():
 r=routes();md={'program_title':'牙狼＜ＧＡＲＯ＞ -魔戒ノ花-','genre':'特撮','air_date':'2026-01-01','needs_review':False}
 assert build_routed_dest_path(r,r'B:\Old\record.mp4',md)[0].startswith(ROOT+'\\')
 md['program_title']='牙狼<GARO> -魔戒ノ花-II'
 assert not build_routed_dest_path(r,r'B:\Old\record.mp4',md)[0].startswith(ROOT+'\\')


def test_source_root_plan_keeps_unmapped_series_reason(tmp_path,monkeypatch,capsys):
 import json,sys,yaml
 import make_move_plan_from_inventory as planner
 from mediaops_schema import connect_db,create_schema_if_needed
 from video_pipeline.db.file_identity import ensure_identity_schema
 db=tmp_path/'db.sqlite';con=connect_db(str(db));create_schema_if_needed(con);ensure_identity_schema(con)
 src=ROOT+r'\別作品\別作品.mp4'
 con.execute("insert into paths(path_id,path,created_at,updated_at) values('pid',?,'now','now')",(src,))
 con.execute("insert into path_metadata(path_id,source,data_json,updated_at,program_title,air_date,needs_review) values('pid','human_reviewed','{}','now','別作品','2026-01-01',0)")
 con.commit();con.close()
 inventory=tmp_path/'inventory.jsonl';inventory.write_text(json.dumps({'path':src})+'\n')
 cfg=tmp_path/'routes.yaml';cfg.write_text(yaml.safe_dump({'default_dest':r'B:\VideoLibrary','routes':[],'work_routes':policy()},allow_unicode=True))
 out=tmp_path/'plan.jsonl'
 monkeypatch.setattr(sys,'argv',['plan','--db',str(db),'--inventory',str(inventory),'--source-root',ROOT,'--drive-routes',str(cfg),'--out',str(out)])
 assert planner.main()==0
 result=json.loads(capsys.readouterr().out)
 assert result['planned']==0 and result['preflight_counts']=={'series_work_unmapped':1}
 assert json.loads(out.with_suffix('.conflicts.json').read_text())[0]['reason']=='series_work_unmapped'

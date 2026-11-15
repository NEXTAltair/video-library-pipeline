import run_metadata_batches_promptv1 as mod


def cache():
    result = object.__new__(mod._EpgCache)
    result._by_filename_stem = {}
    result._by_match_key = {}
    result._by_title_dt = {}
    result._by_datetime_key = {}
    return result


def test_recording_stem_ignores_windows_directories_on_linux():
    assert mod.recording_filename_stem(r'B:\VideoLibrary\Show\Show_2026_11_01_21_00.mp4') == 'Show_2026_11_01_21_00'
    assert mod.recording_filename_stem('/mnt/b/Show_2026_11_01_21_00.mp4') == 'Show_2026_11_01_21_00'


def test_simultaneous_other_program_never_enriches_metadata():
    c = cache()
    c._by_datetime_key['2026-11-01 21:00'] = [
        {'official_title': 'Unrelated documentary', 'epg_genres': [{'category': 'ドキュメンタリー'}]},
        {'official_title': 'VIVANT', 'epg_genres': [{'category': 'ドラマ'}]},
    ]
    assert c.lookup(None, '2026-11-01 21:00', 'VIVANT_2026_11_01_21_00') is None


def test_timestamp_alone_is_insufficient_even_for_one_cached_broadcast():
    c = cache()
    c._by_datetime_key['slot'] = [{'official_title': 'Other'}]
    assert c.lookup(None, 'slot') is None


def test_exact_recording_stem_wins_over_other_channel_in_same_slot():
    c = cache()
    actual = {'official_title': 'VIVANT', 'epg_genres': [{'category': 'ドラマ'}]}
    c._by_filename_stem['VIVANT_2026_11_01_21_00'] = actual
    c._by_datetime_key['slot'] = [{'official_title': 'Other'}]
    stem = mod.recording_filename_stem(r'B:\VideoLibrary\日曜劇場\VIVANT_2026_11_01_21_00.mp4')
    assert c.lookup(None, 'slot', stem) is actual


def test_exact_work_and_datetime_match_remains_supported():
    c = cache()
    actual = {'official_title': 'Actual'}
    c._by_match_key['actual-slot-channel'] = actual
    assert c.lookup('actual-slot-channel', 'slot') is actual


def test_full_episode_title_prefix_can_disambiguate_simultaneous_broadcasts():
    c = cache()
    actual = {'official_title': '日曜劇場「VIVANT」第22話 明かされる正体'}
    c._by_datetime_key['slot'] = [{'official_title': 'NHKスペシャル 全く別の番組'}, actual]
    assert c.lookup(None, 'slot', '日曜劇場「VIVANT」第22話_明かされる正体_説明_2026_10_25_21_45') is actual
    c._by_datetime_key['slot'].append(dict(actual))
    assert c.lookup(None, 'slot', '日曜劇場「VIVANT」第22話_明かされる正体_説明') is None


def test_verified_drama_slot_supplies_genre_without_borrowing_another_channel():
    from genre_resolver import resolve_genre
    assert resolve_genre({'program_title': 'VIVANT', 'evidence': {'raw': '日曜劇場「VIVANT」第23話'}}) == 'ドラマ'
    assert resolve_genre({'program_title': '孤独のグルメSeason11', 'evidence': {'raw': 'ドラマ24孤独のグルメSeason11 5話'}}) == 'ドラマ'
    assert resolve_genre({'program_title': '地球ドラマチック'}) == 'ドキュメンタリー・情報'
    assert resolve_genre({'program_title': 'VIVANT'}) is None


def test_channel_disambiguates_same_title_broadcast_simultaneously():
    c = cache()
    a = {'official_title': '日曜劇場「VIVANT」第22話', 'broadcaster': '局A'}
    b = dict(a, broadcaster='局B')
    c._by_title_dt['work-slot'] = [a, b]
    assert c.lookup('work-slot', 'slot', broadcaster='局B') is b
    assert c.lookup('work-slot', 'slot') is None
    assert c.lookup('work-slot', 'slot', broadcaster='局C') is None

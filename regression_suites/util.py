import datetime
import json
import os
import time
from uuid import UUID

import requests

GITHUB_TAGS = "https://api.github.com/repos/apache/cassandra/git/refs/tags"
GITHUB_BRANCHES = "https://api.github.com/repos/apache/cassandra/branches"
KNOWN_SERIES = tuple(('no_series',
                      'daily_regressions_trunk-compaction',
                      'daily_regressions_trunk-repair_10M',
                      'daily_regressions_trunk-compaction_stcs',
                      'daily_regressions_trunk-compaction_dtcs',
                      'daily_regressions_trunk-compaction_lcs',
                      'daily_regressions_trunk-commitlog_sync',
                      ))


def copy_and_update(d1, d2):
    r = d1.copy()
    r.update(d2)
    return r


def get_shas_from_stats(stats):
    """
    Given a stats dictionary, such as would be returned from calling json.loads
    on the value returned from the /stats endpoint on a cstar_perf test,
    return a dictionary mapping from the revisions to the SHA used for the
    test.
    """
    revisions_with_git_id = [r for r in stats['revisions'] if
                             ('revision' in r
                              and 'git_id' in r
                              and r['git_id'])]
    revisions_collapsed_git_id = [copy_and_update(r, {'git_id': (set(r['git_id'].values())
                                                                 if isinstance(r['git_id'], dict)
                                                                 else {r['git_id']})})
                                  for r in revisions_with_git_id]
    rv = {r['revision']: r['git_id'] for r in revisions_collapsed_git_id}
    print 'collapsed to {}'.format(rv)

    return rv


def uuid_to_datetime(uid):
    return datetime.datetime.fromtimestamp((uid.get_time() - 0x01b21dd213814000L)*100/1e9)


def uuid_absolute_distance_from_datetime(ref_dt):
    def absolute_distance_from_ref_datetime(cmp_uuid):
        return abs(ref_dt - uuid_to_datetime(cmp_uuid))
    return absolute_distance_from_ref_datetime


def get_cstar_jobs_uuids(cstar_server, series=None):
    if series is None:
        uuids_file = os.path.join(os.getcwd(), os.path.dirname(__file__), 'all-uuids.txt')
        with open(uuids_file) as f:
            uuids = list(line.strip() for line in f.readlines())
    else:
        series_url = '/'.join([cstar_server, 'api', 'series', series,
                               str(1), str(int(time.time()))])
        series_uuids = None
        try:
            series_uuids = requests.get(series_url)
        except requests.exceptions.ConnectionError as e:
            print "Can't get series uuids: {}".format(e)

        uuids = json.loads(series_uuids.text)['series']

    return uuids


def get_sha_from_build_days_ago(cstar_server, days_ago, revision):
    print 'getting sha from {}'.format(revision)

    test_uuids = []
    for series in [None] + list(KNOWN_SERIES):
        uuids_from_series = get_cstar_jobs_uuids(cstar_server=cstar_server, series=series)
        if uuids_from_series:
            test_uuids.extend(uuids_from_series)
    test_uuids = list(map(UUID, ['{' + u + '}' for u in test_uuids]))

    td = datetime.datetime.now() - datetime.timedelta(days=days_ago)
    print 'finding sha closest to {}'.format(td)
    test_ids_by_distance_asc = list(sorted(test_uuids,
                                           key=uuid_absolute_distance_from_datetime(td)))

    for test_id in test_ids_by_distance_asc[:30]:
        print 'trying {}'.format(test_id)
        stats_url = '/'.join(
            [cstar_server, 'tests', 'artifacts', str(test_id), 'stats', 'stats.{}.json'.format(str(test_id))])
        try:
            stats_json = requests.get(stats_url).text
        except requests.exceptions.ConnectionError as e:
            print "didn't work :( {}".format(e)
            continue
        try:
            stats_data = json.loads(stats_json)
        except ValueError as e:
            print "didn't work :( {}".format(e)
            continue

        shas = get_shas_from_stats(stats_data)
        sha_set = shas.get(revision)
        if sha_set and len(sha_set) == 1:
            sha = next(iter(sha_set))
            print '    appending {}'.format(sha)
            return sha

# when executing this file and not importing it, run the tests
if __name__ == '__main__':
    example_data = json.loads("""
    {
        "revisions": [
            {
                "env": "",
                "git_id": {
                    "blade-11-6a": "5d38559908cfd8b5ecbad03a1cedb355d7856cee",
                    "blade-11-7a": "5d38559908cfd8b5ecbad03a1cedb355d7856cee",
                    "blade-11-8a": "5d38559908cfd8b5ecbad03a1cedb355d7856cee",
                    "ryan@blade-11-5a": "5d38559908cfd8b5ecbad03a1cedb355d7856cee"
                },
                "java_home": "~/fab/jvms/jdk1.8.0_45",
                "label": null,
                "last_log": "16d8514c-67a7-11e5-888c-002590892848",
                "options": {
                    "use_vnodes": true
                },
                "product": "cassandra",
                "revision": "apache/trunk",
                "yaml": ""
            },
            {
                "env": "",
                "git_id": {
                    "blade-11-6a": "3bfeba37a6ccde99fba3170cb5eac977a566db30",
                    "blade-11-7a": "3bfeba37a6ccde99fba3170cb5eac977a566db30",
                    "blade-11-8a": "3bfeba37a6ccde99fba3170cb5eac977a566db30",
                    "ryan@blade-11-5a": "3bfeba37a6ccde99fba3170cb5eac977a566db30"
                },
                "java_home": "~/fab/jvms/jdk1.8.0_45",
                "label": null,
                "last_log": "042d405e-67aa-11e5-888c-002590892848",
                "options": {
                    "use_vnodes": true
                },
                "product": "cassandra",
                "revision": "tjake/rxjava-3.0",
                "yaml": ""
            }
        ],
        "stats": [],
        "subtitle": "",
        "title": "rxjava 3.0 test 1"
    }
    """)
    assert get_shas_from_stats(example_data) == {
        'apache/trunk': ['5d38559908cfd8b5ecbad03a1cedb355d7856cee'],
        'tjake/rxjava-3.0': ['3bfeba37a6ccde99fba3170cb5eac977a566db30']
    }

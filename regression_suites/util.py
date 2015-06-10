import re
import urllib2
import json
from distutils.version import LooseVersion
import datetime
import os
from uuid import UUID
import requests


GITHUB_TAGS = "https://api.github.com/repos/apache/cassandra/git/refs/tags"
GITHUB_BRANCHES = "https://api.github.com/repos/apache/cassandra/branches"

# highest and lowest UUIDs, as sorted by C*; va pycassa source
LOWEST_TIME_UUID = UUID('00000000-0000-1000-8080-808080808080')
HIGHEST_TIME_UUID = UUID('ffffffff-ffff-1fff-bf7f-7f7f7f7f7f7f')


def get_tagged_releases(series='stable'):
    """Retrieve git tags and find version numbers for a release series

    series - 'stable', 'oldstable', or 'testing'"""
    releases = []
    if series == 'testing':
        # Testing releases always have a hyphen after the version number:
        tag_regex = re.compile('^refs/tags/cassandra-([0-9]+\.[0-9]+\.[0-9]+-.*$)')
    else:
        # Stable and oldstable releases are just a number:
        tag_regex = re.compile('^refs/tags/cassandra-([0-9]+\.[0-9]+\.[0-9]+$)')

    r = urllib2.urlopen(GITHUB_TAGS)
    for ref in (i.get('ref', '') for i in json.loads(r.read())):
        m = tag_regex.match(ref)
        if m:
            releases.append(LooseVersion(m.groups()[0]))

    # Sort by semver:
    releases.sort(reverse=True)

    stable_major_version = LooseVersion(str(releases[0].version[0]) + "." + str(releases[0].version[1]))
    stable_releases = [release for release in releases if r >= stable_major_version]
    oldstable_releases = [release for release in releases if r not in stable_releases]
    oldstable_major_version = LooseVersion(str(oldstable_releases[0].version[0]) + "." + str(oldstable_releases[0].version[1]))
    oldstable_releases = [release for release in oldstable_releases if r >= oldstable_major_version]

    if series == 'testing':
        return ['cassandra-' + release.vstring for release in releases]
    elif series == 'stable':
        return ['cassandra-'+release.vstring for release in stable_releases]
    elif series == 'oldstable':
        return ['cassandra-'+release.vstring for release in oldstable_releases]
    else:
        raise AssertionError("unknown release series: {series}".format(series=series))


def get_branches():
    """Retrieve branch names in release sorted order

    Does not include trunk.

    eg : ['cassandra-3.0','cassandra-2.1','cassandra-2.0','cassandra-1.2']"""
    branches = []
    branch_regex = re.compile('^cassandra-([0-9]+\.[0-9]+$)')

    r = urllib2.urlopen(GITHUB_BRANCHES)
    data = json.loads(r.read())
    for name in (i.get('name', '') for i in data):
        m = branch_regex.match(name)
        if m:
            branches.append(LooseVersion(m.groups()[0]))

    # Sort by semver:
    branches.sort(reverse=True)

    return ['apache/cassandra-'+b.vstring for b in branches]


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
    # someday, this will make a call to cstar_perf. that day is not today.
    uuids_file = os.path.join(os.getcwd(), os.path.dirname(__file__), 'all-uuids.txt')
    with open(uuids_file) as f:
        uuids = list(line.strip() for line in f.readlines())
    if series:
        series_url = '/'.join([cstar_server, 'api', 'series', series,
                               str(LOWEST_TIME_UUID), str(HIGHEST_TIME_UUID)])
        series_uuids = None
        try:
            series_uuids = requests.get(series_url)
        except requests.exceptions.ConnectionError as e:
            print "Can't get series uuids: {}".format(e)

        if series_uuids:
            uuids += series_uuids

    return uuids


def get_sha_from_build_days_ago(cstar_server, day_deltas, revision, series=None):
    print 'getting sha from {}'.format(revision)
    test_uuids = [UUID(i) for i in
                  get_cstar_jobs_uuids(cstar_server=cstar_server, series=series)]

    closest_shas = []

    for days_ago in day_deltas:
        td = datetime.datetime.now() - datetime.timedelta(days=days_ago)
        print 'finding sha closest to {}'.format(td)
        test_ids_by_distance_asc = list(sorted(test_uuids,
                                               key=uuid_absolute_distance_from_datetime(td)))

        for test_id in test_ids_by_distance_asc[:30]:
            print 'trying {}'.format(test_id)
            stats_url = '/'.join([cstar_server, 'tests', 'artifacts', str(test_id), 'stats'])
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
                closest_shas.append(sha)
                break

    return closest_shas

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

import re
import urllib2
import json
from distutils.version import LooseVersion

GITHUB_TAGS="https://api.github.com/repos/apache/cassandra/git/refs/tags"
GITHUB_BRANCHES="https://api.github.com/repos/apache/cassandra/branches"

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
    for ref in (i.get('ref','') for i in json.loads(r.read())) :
        m = tag_regex.match(ref)
        if m:
            releases.append(LooseVersion(m.groups()[0]))

    # Sort by semver:
    releases.sort(reverse=True)

    stable_major_version = LooseVersion(str(releases[0].version[0]) + "." + str(releases[0].version[1]))
    stable_releases = [r for r in releases if r >= stable_major_version]
    oldstable_releases = [r for r in releases if r not in stable_releases]
    oldstable_major_version = LooseVersion(str(oldstable_releases[0].version[0]) + "." + str(oldstable_releases[0].version[1]))
    oldstable_releases = [r for r in oldstable_releases if r >= oldstable_major_version]

    if series == 'testing':
        return ['cassandra-'+r.vstring for r in releases]
    elif series == 'stable':
        return ['cassandra-'+r.vstring for r in stable_releases]
    elif series == 'oldstable':
        return ['cassandra-'+r.vstring for r in oldstable_releases]
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
    for name in (i.get('name','') for i in data) :
        m = branch_regex.match(name)
        if m:
            branches.append(LooseVersion(m.groups()[0]))

    # Sort by semver:
    branches.sort(reverse=True)

    return ['cassandra-'+b.vstring for b in branches]

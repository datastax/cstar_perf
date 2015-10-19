import datetime
from util import get_sha_from_build_days_ago
from cstar_perf.frontend.client.schedule import Scheduler
# import requests

REVISION = 'apache/trunk'
CSTAR_SERVER = "cstar.datastax.com"
day_deltas = [7, 14]
OLD_SHAS = dict(zip(day_deltas,
                    get_sha_from_build_days_ago('http://' + CSTAR_SERVER,
                                                day_deltas=day_deltas,
                                                revision=REVISION)))
DEFAULT_CLUSTER_NAME = 'blade_11_b'


def create_baseline_config(title=None, series=None):
    """Creates a config for testing the latest dev build(s) against stable and oldstable"""

    dev_revisions = dict({0: REVISION}, **OLD_SHAS)

    config = {}

    config['revisions'] = []
    config['testseries'] = 'daily_regressions_trunk'
    for days_ago, revision in sorted(dev_revisions.items()):
        label = REVISION if days_ago == 0 else '{REVISION} ~{days_ago} days ago'.format(REVISION=REVISION,
                                                                                        days_ago=days_ago)
        config['revisions'].append({'revision': revision, 'label': label})
    for r in config['revisions']:
        r['options'] = {'use_vnodes': True}
        r['java_home'] = ("~/fab/jvms/jdk1.7.0_71"
                          if 'oldstable' in r['label']
                          else "~/fab/jvms/jdk1.8.0_45")

    config['title'] = 'Daily C* regression suite - {}'.format(datetime.datetime.now().strftime("%Y-%m-%d"))
    config['product'] = 'cassandra'

    if title is not None:
        config['title'] += ' - {title}'.format(title=title)
    if series is not None:
        config['testseries'] += '-' + series

    return config


def test_simple_profile(title='Read/Write', cluster=DEFAULT_CLUSTER_NAME, load_rows=65000000, read_rows=65000000, threads=300, yaml=None, series='read_write'):
    """Test the basic stress profile with default C* settings"""
    config = create_baseline_config(title, series)
    config['cluster'] = cluster
    config['operations'] = [
        {'operation': 'stress',
         'command': 'write n={load_rows} -rate threads={threads}'.format(**locals())},
        {'operation': 'stress',
         'command': 'read n={read_rows} -rate threads={threads}'.format(**locals())},
        {'operation': 'stress',
         'command': 'read n={read_rows} -rate threads={threads}'.format(**locals())}
    ]
    if yaml:
        config['yaml'] = yaml

    scheduler = Scheduler(CSTAR_SERVER)
    scheduler.schedule(config)


def compaction_profile(title='Compaction', cluster=DEFAULT_CLUSTER_NAME, rows=65000000, threads=300):
    config = create_baseline_config(title, 'compaction')
    config['cluster'] = cluster
    config['operations'] = [
        {'operation': 'stress',
         'command': 'write n={rows} -rate threads={threads}'.format(rows=rows, threads=threads)},
        {'operation': 'nodetool',
         'command': 'flush'},
        {'operation': 'nodetool',
         'command': 'compact'},
        {'operation': 'stress',
         'command': 'read n={rows} -rate threads={threads}'.format(rows=rows, threads=threads)},
        {'operation': 'stress',
         'command': 'read n={rows} -rate threads={threads}'.format(rows=rows, threads=threads)}]

    scheduler = Scheduler(CSTAR_SERVER)
    scheduler.schedule(config)


def test_compaction_profile():
    compaction_profile(rows='10M')


def repair_profile(title='Repair', cluster=DEFAULT_CLUSTER_NAME, rows=65000000, threads=300, series=None):
    config = create_baseline_config(title, series)
    config['cluster'] = cluster
    config['operations'] = [
        {'operation': 'stress',
         'command': 'write n={rows} -rate threads={threads}'.format(rows=rows, threads=threads)},
        {'operation': 'nodetool',
         'command': 'flush'},
        {'operation': 'nodetool',
         'command': 'repair'},
        {'operation': 'stress',
         'command': 'read n={rows} -rate threads={threads}'.format(rows=rows, threads=threads)},
        {'operation': 'stress',
         'command': 'read n={rows} -rate threads={threads}'.format(rows=rows, threads=threads)}]

    scheduler = Scheduler(CSTAR_SERVER)
    scheduler.schedule(config)


def test_repair_profile():
    repair_profile(rows='10M', series='repair_10M')


def compaction_strategies_profile(title='Compaction Strategy', cluster=DEFAULT_CLUSTER_NAME, rows=65000000, threads=300, strategy=None, series=None):
    config = create_baseline_config(title, series)
    config['cluster'] = cluster

    schema_options = 'replication\(factor=3\)'
    if strategy:
        schema_options += ' compaction\(strategy={strategy}\)'.format(strategy=strategy)

    config['operations'] = [
        {'operation': 'stress',
         'command': 'write n={rows} -rate threads={threads} '
                    '-schema {schema_options}'.format(rows=rows, threads=threads,
                                                      schema_options=schema_options)},
        {'operation': 'nodetool',
         'command': 'flush'},
        {'operation': 'nodetool',
         'command': 'compact'},
        {'operation': 'stress',
         'command': 'read n={rows} -rate threads={threads}'.format(rows=rows, threads=threads)},
        {'operation': 'stress',
         'command': 'read n={rows} -rate threads={threads}'.format(rows=rows, threads=threads)}]

    scheduler = Scheduler(CSTAR_SERVER)
    scheduler.schedule(config)


def test_STCS_profile():
    compaction_strategies_profile(title='STCS', strategy='org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
                                  rows='10M', series='compaction_stcs')


def test_DTCS_profile():
    compaction_strategies_profile(title='DTCS', strategy='org.apache.cassandra.db.compaction.DateTieredCompactionStrategy',
                                  rows='10M', series='compaction_dtcs')


def test_LCS_profile():
    compaction_strategies_profile(title='LCS', strategy='org.apache.cassandra.db.compaction.LeveledCompactionStrategy',
                                  rows='10M', series='compaction_lcs')


def test_commitlog_sync_settings():
    yaml = '\n'.join(['commitlog_sync: batch',
                      'commitlog_sync_batch_window_in_ms: 2',
                      'commitlog_sync_period_in_ms: null',
                      'concurrent_writes: 64'])
    test_simple_profile(title='Batch Commitlog', yaml=yaml,
                        load_rows='10M', read_rows='10M', series='commitlog_sync')

import datetime
from util import get_sha_from_build_days_ago, get_tagged_releases
from cstar_perf.frontend.client.schedule import Scheduler
# import json

REVISION = 'apache/trunk'
CSTAR_SERVER = "cstar.datastax.com"
DEFAULT_CLUSTER_NAME = 'blade_11_b'
NODES = ['blade-11-6a', 'blade-11-7a', 'blade-11-8a']

# class Scheduler(object):
#     def __init__(self, server):
#         self.server = server
#
#     def schedule(self, config):
#         print(json.dumps(config, sort_keys=True, indent=4, separators=(',', ': ')))


def stable_oldstable_revisions():
    stable = get_tagged_releases('stable')[0]
    oldstable = get_tagged_releases('oldstable')[0]
    return [
        {
            "revision": "refs/tags/{}".format(stable),
            "label": "Stable: {}".format(stable)
        },
        {
            "revision": "refs/tags/{}".format(oldstable),
            "label": "Old Stable: {}".format(oldstable)
        }
    ]


def rolling_window_revisions():
    day_deltas = [7, 14]
    old_shas = dict(zip(day_deltas,
                        get_sha_from_build_days_ago('http://' + CSTAR_SERVER,
                                                    day_deltas=day_deltas,
                                                    revision=REVISION)))

    dev_revisions = dict({0: REVISION}, **old_shas)
    revisions = []
    for days_ago, revision in sorted(dev_revisions.items()):
        label = REVISION if days_ago == 0 else '{REVISION} ~{days_ago} days ago'.format(REVISION=REVISION,
                                                                                        days_ago=days_ago)
        revisions.append({'revision': revision, 'label': label})
    return revisions


def rolling_upgrade_version_revisions():
    stable = get_tagged_releases('stable')[0]
    oldstable = get_tagged_releases('oldstable')[0]
    return [
        {
            "revision": "refs/tags/{}".format(oldstable),
            "label": "step_0_  3 Old Stable Nodes: {}".format(oldstable),
            "cluster_name": "updating_cluster"
        },
        {
            "revision": "refs/tags/{}".format(oldstable),
            "label": "step_1_  2 Old Stable Nodes, 1 Stable: {}/{}".format(oldstable, stable),
            "revision_override": {"refs/tags/{}".format(stable): [NODES[0]]},
            "cluster_name": "updating_cluster"
        },
        {
            "revision": "refs/tags/{}".format(oldstable),
            "label": "step_2_  1 Old Stable Nodes, 2 Stable: {}/{}".format(oldstable, stable),
            "revision_override": {"refs/tags/{}".format(stable): [NODES[0], NODES[1]]},
            "cluster_name": "updating_cluster"
        },
        {
            "revision": "refs/tags/{}".format(stable),
            "label": "step_3_  3 Stable Nodes: {}".format(stable),
            "cluster_name": "updating_cluster"
        },
        {
            "revision": "refs/tags/{}".format(stable),
            "label": "step_4_  2 Stable Nodes, 1 Trunk: {}".format(stable),
            "revision_override": {'apache/trunk': [NODES[0]]},
            "cluster_name": "updating_cluster"
        },
        {
            "revision": "refs/tags/{}".format(stable),
            "label": "step_5_  1 Stable Nodes, 2 Trunk: {}".format(stable),
            "revision_override": {'apache/trunk': [NODES[0], NODES[1]]},
            "cluster_name": "updating_cluster"
        },
        {
            "revision": "apache/trunk",
            "label": "step_6_  3 Trunk Nodes",
            "cluster_name": "updating_cluster"
        }
    ]


def standard_rolling_window_revisions():
    return rolling_window_revisions() + stable_oldstable_revisions()


def create_baseline_config(title=None, series=None, revisions=standard_rolling_window_revisions()):
    """Creates a config for testing the latest dev build(s) against stable and oldstable"""

    config = {
        'revisions': revisions,
        'testseries': 'daily_regressions_trunk'
    }

    for r in config['revisions']:
        r['options'] = {'use_vnodes': True, 'token_allocation': 'random'}
        r['java_home'] = ("~/fab/jvms/jdk1.7.0_71"
                          if 'oldstable' in r['label']
                          else "~/fab/jvms/jdk1.8.0_45")
        r['product'] = 'cassandra'
        r['yourkit_profiler'] = False

    config['title'] = 'Daily C* regression suite - {}'.format(datetime.datetime.now().strftime("%Y-%m-%d"))
    config['product'] = 'cassandra'

    if title is not None:
        config['title'] += ' - {title}'.format(title=title)
    if series is not None:
        config['testseries'] += '-' + series

    return config


# def test_upgrading_versions(title='RollingUpgrade', cluster=DEFAULT_CLUSTER_NAME, series='rolling_upgrade'):
#     config = create_baseline_config(title, series, rolling_upgrade_version_revisions())
#     config['cluster'] = cluster
#     config['num_nodes'] = "3",
#     config['leave_data'] = True
#     config['operations'] = [
#         {
#             'operation': 'nodetool',
#             'command': 'version'
#         },
#         {
#             'operation': 'nodetool',
#             'command': 'upgradesstables',
#             "wait_for_compaction": True
#         },
#         {
#             'operation': 'nodetool',
#             'command': 'status'
#         },
#         {
#             'operation': 'stress',
#             "command": "write n=25M -rate threads=100",
#             "wait_for_compaction": True,
#             'nodes': NODES
#         },
#         {
#             'operation': 'stress',
#             "command": "read n=25M -rate threads=100",
#             "wait_for_compaction": True,
#             'nodes': NODES
#         },
#         {
#             'operation': 'nodetool',
#             'command': 'flush',
#             "wait_for_compaction": True
#         },
#     ]

#     scheduler = Scheduler(CSTAR_SERVER)
#     scheduler.schedule(config)


# def test_simple_profile(title='Read/Write', cluster=DEFAULT_CLUSTER_NAME, load_rows='65M', read_rows='65M',
#                         threads=300, yaml=None, series='read_write'):
#     """Test the basic stress profile with default C* settings"""
#     config = create_baseline_config(title, series)
#     config['cluster'] = cluster
#     config['operations'] = [
#         {'operation': 'stress',
#          'stress_revision': 'apache/trunk',
#          'command': 'write n={load_rows} -rate threads={threads}'.format(**locals()),
#          'wait_for_compaction': True},
#         {'operation': 'stress',
#          'stress_revision': 'apache/trunk',
#          'command': 'read n={read_rows} -rate threads={threads}'.format(**locals()),
#          'wait_for_compaction': True},
#         {'operation': 'stress',
#          'stress_revision': 'apache/trunk',
#          'command': 'read n={read_rows} -rate threads={threads}'.format(**locals()),
#          'wait_for_compaction': True}
#     ]
#     if yaml:
#         config['yaml'] = yaml

#     scheduler = Scheduler(CSTAR_SERVER)
#     scheduler.schedule(config)


# def compaction_profile(title='Compaction', cluster=DEFAULT_CLUSTER_NAME, rows='65M', threads=300):
#     config = create_baseline_config(title, 'compaction')
#     config['cluster'] = cluster
#     config['operations'] = [
#         {'operation': 'stress',
#          'stress_revision': 'apache/trunk',
#          'command': 'write n={rows} -rate threads={threads}'.format(rows=rows, threads=threads),
#          'wait_for_compaction': True},
#         {'operation': 'nodetool',
#          'command': 'flush'},
#         {'operation': 'nodetool',
#          'command': 'compact'},
#         {'operation': 'stress',
#          'stress_revision': 'apache/trunk',
#          'command': 'read n={rows} -rate threads={threads}'.format(rows=rows, threads=threads),
#          'wait_for_compaction': True},
#         {'operation': 'stress',
#          'stress_revision': 'apache/trunk',
#          'command': 'read n={rows} -rate threads={threads}'.format(rows=rows, threads=threads),
#          'wait_for_compaction': True}
#     ]

#     scheduler = Scheduler(CSTAR_SERVER)
#     scheduler.schedule(config)


# def test_compaction_profile():
#     compaction_profile(rows='10M')


# def repair_profile(title='Repair', cluster=DEFAULT_CLUSTER_NAME, rows='65M', threads=300, series=None):
#     config = create_baseline_config(title, series)
#     config['cluster'] = cluster
#     config['operations'] = [
#         {'operation': 'stress',
#          'stress_revision': 'apache/trunk',
#          'command': 'write n={rows} -rate threads={threads}'.format(rows=rows, threads=threads),
#          'wait_for_compaction': True},
#         {'operation': 'nodetool',
#          'command': 'flush'},
#         {'operation': 'nodetool',
#          'command': 'repair'},
#         {'operation': 'stress',
#          'stress_revision': 'apache/trunk',
#          'command': 'read n={rows} -rate threads={threads}'.format(rows=rows, threads=threads),
#          'wait_for_compaction': True},
#         {'operation': 'stress',
#          'stress_revision': 'apache/trunk',
#          'command': 'read n={rows} -rate threads={threads}'.format(rows=rows, threads=threads),
#          'wait_for_compaction': True}
#     ]

#     scheduler = Scheduler(CSTAR_SERVER)
#     scheduler.schedule(config)


# def test_repair_profile():
#     repair_profile(rows='10M', series='repair_10M')


# def compaction_strategies_profile(title='Compaction Strategy', cluster=DEFAULT_CLUSTER_NAME, rows='65M',
#                                   threads=300, strategy=None, series=None):
#     config = create_baseline_config(title, series)
#     config['cluster'] = cluster
#     for r in config['revisions']:
#         r['options'] = {'use_vnodes': False, 'token_allocation': 'non-vnodes'}

#     schema_options = 'replication\(factor=3\)'
#     if strategy:
#         schema_options += ' compaction\(strategy={strategy}\)'.format(strategy=strategy)

#     config['operations'] = [
#         {
#             'operation': 'stress',
#             'stress_revision': 'apache/trunk',
#             'command': 'write n={rows} cl=QUORUM -rate threads={threads} -schema {schema_options}'
#                 .format(rows=rows, threads=threads, schema_options=schema_options),
#             'wait_for_compaction': True
#         },
#         {
#             'operation': 'nodetool',
#             'command': 'flush',
#             'wait_for_compaction': True
#         },
#         {
#             'operation': 'nodetool',
#             'command': 'compact',
#             'wait_for_compaction': True
#         },
#         {
#             'operation': 'stress',
#             'stress_revision': 'apache/trunk',
#             'command': 'read n={rows} cl=QUORUM -rate threads={threads}'.format(rows=rows, threads=threads),
#             'wait_for_compaction': True
#         },
#         {
#             'operation': 'stress',
#             'stress_revision': 'apache/trunk',
#             'command': 'read n={rows} cl=QUORUM -rate threads={threads}'.format(rows=rows, threads=threads),
#             'wait_for_compaction': True
#         }
#     ]

#     scheduler = Scheduler(CSTAR_SERVER)
#     scheduler.schedule(config)


# def test_STCS_profile():
#     compaction_strategies_profile(title='STCS', strategy='org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
#                                   rows='10M', series='compaction_stcs')


# def test_DTCS_profile():
#     compaction_strategies_profile(title='DTCS', strategy='org.apache.cassandra.db.compaction.DateTieredCompactionStrategy',
#                                   rows='10M', series='compaction_dtcs')


# def test_LCS_profile():
#     compaction_strategies_profile(title='LCS', strategy='org.apache.cassandra.db.compaction.LeveledCompactionStrategy',
#                                   rows='10M', series='compaction_lcs')


# def test_commitlog_sync_settings():
#     yaml = '\n'.join(['commitlog_sync: batch',
#                       'commitlog_sync_batch_window_in_ms: 2',
#                       'commitlog_sync_period_in_ms: null',
#                       'concurrent_writes: 64'])
#     test_simple_profile(title='Batch Commitlog', yaml=yaml,
#                         load_rows='10M', read_rows='10M', series='commitlog_sync')


def test_materialized_view_3_mv(title='Materialized Views (3 MV)', cluster=DEFAULT_CLUSTER_NAME,
                           rows='50M', threads=300, series='materialized_views_write_3_mv'):
    config = create_baseline_config(title, series, rolling_window_revisions())
    config['cluster'] = cluster
    config['operations'] = [
        {'operation': 'stress',
         'stress_revision': 'apache/trunk',
         'command': ('user profile=http://cassci.datastax.com/userContent/cstar_perf_regression/users-rf3-3mv.yaml '
                     'ops\(insert=1\) n={rows}M -rate threads={threads}').format(rows=rows, threads=threads),
         'wait_for_compaction': False}
    ]

    scheduler = Scheduler(CSTAR_SERVER)
    scheduler.schedule(config)


def test_materialized_view_1_mv(title='Materialized Views (1 MV)', cluster=DEFAULT_CLUSTER_NAME,
                           rows='50M', threads=300, series='materialized_views_write_1_mv'):
    config = create_baseline_config(title, series, rolling_window_revisions())
    config['cluster'] = cluster
    config['operations'] = [
        {'operation': 'stress',
         'stress_revision': 'apache/trunk',
         'command': ('user profile=http://cassci.datastax.com/userContent/cstar_perf_regression/users-rf3-1mv.yaml '
                     'ops\(insert=1\) n={rows}M -rate threads={threads}').format(rows=rows, threads=threads),
         'wait_for_compaction': False}
    ]

    scheduler = Scheduler(CSTAR_SERVER)
    scheduler.schedule(config)

import datetime

from cstar_perf.frontend.client.schedule import Scheduler

CSTAR_SERVER = "cstar.datastax.com"


def create_baseline_config(title=None):
    """Creates a config for testing the latest dev build(s) against stable and oldstable"""

    dev_revisions = ['apache/trunk', 'apache/cassandra-3.0', 'apache/cassandra-2.2']
    config = {}
    config['revisions'] = revisions = []

    for r in dev_revisions:
        revisions.append({'revision': r, 'label': r + ' (dev)'})

    for r in revisions:
        r['options'] = {'use_vnodes': True}
        r['java_home'] = "~/fab/jvms/jdk1.7.0_71" if 'oldstable' in r['label'] else "~/fab/jvms/jdk1.8.0_45"

    config['title'] = 'Jenkins C* regression suite - {}'.format(datetime.datetime.now().strftime("%Y-%m-%d"))

    if title is not None:
        config['title'] += ' - {title}'.format(title=title)

    return config


def test_simple_profile(title='Read/Write', cluster='blade_11', load_rows=65000000, read_rows=65000000, threads=300, yaml=None):
    """Test the basic stress profile with default C* settings"""
    config = create_baseline_config(title)
    config['cluster'] = cluster
    config['operations'] = [
        {'operation':'stress',
         'command': 'write n={load_rows} -rate threads={threads}'.format(**locals())},
        {'operation':'stress',
         'command': 'read n={read_rows} -rate threads={threads}'.format(**locals())},
        {'operation':'stress',
         'command': 'read n={read_rows} -rate threads={threads}'.format(**locals())}
    ]
    if yaml:
        config['yaml'] = yaml

    scheduler = Scheduler(CSTAR_SERVER)
    scheduler.schedule(config)

def compaction_profile(title='Compaction', cluster='blade_11', rows=65000000, threads=300):
    config = create_baseline_config(title)
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
    compaction_profile()

def repair_profile(title='Repair', cluster='blade_11', rows=65000000, threads=300):
    config = create_baseline_config(title)
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
    repair_profile()

def compaction_strategies_profile(title='Compaction Strategy', cluster='blade_11', rows=65000000, threads=300, strategy=None):
    config = create_baseline_config(title)
    config['cluster'] = cluster

    schema_options = 'replication=\(factor=3\)'
    if strategy:
        schema_options += ' compaction\(strategy={strategy}'.format(strategy=strategy)

    config['operations'] = [
        {'operation': 'stress',
         'command': 'write n={rows} -rate threads={threads} -schema {schema_options}'.format(rows=rows, threads=threads,
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
    compaction_strategies_profile(title='STCS', strategy='SizeTieredCompactionStrategy')

def test_DTCS_profile():
    compaction_strategies_profile(title='DTCS', strategy='DateTieredCompactionStrategy')

def test_LCS_profile():
    compaction_strategies_profile(title='LCS', strategy='LeveledCompactionStrategy')

def test_commitlog_sync_settings():
    yaml = '\n'.join(['commitlog_sync: batch',
                      'commitlog_sync_batch_window_in_ms: 2',
                      'commitlog_sync_period_in_ms: null',
                      'concurrent_writes: 64'])
    test_simple_profile(title='Batch Commitlog', yaml=yaml)

def number_of_columns_profile(title='Read/Write {columns}', cluster='blade_11', rows=65000000, threads=300, columns=50):
    config = create_baseline_config(title.format(columns=columns))
    config['cluster'] = cluster
    config['operations'] = [
        {'operation':'stress',
         'command': 'write n={rows} -rate threads={threads} -col n=FIXED\({columns}\)'.format(rows=rows, threads=threads, columns=columns)},
        {'operation':'stress',
         'command': 'read n={rows} -rate threads={threads}'.format(rows=rows, threads=threads)},
        {'operation':'stress',
         'command': 'read n={rows} -rate threads={threads}'.format(rows=rows, threads=threads)}
    ]

    scheduler = Scheduler(CSTAR_SERVER)
    scheduler.schedule(config)

def test_fifty_columns_profile():
    number_of_columns_profile(columns=50)

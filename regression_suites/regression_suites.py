import datetime
from util import get_tagged_releases, get_branches
from cstar_perf.frontend.client.schedule import Scheduler

CSTAR_SERVER = "cstar.datastax.com"

def create_baseline_config():
    """Creates a config for testing the latest dev build(s) against stable and oldstable"""
    
    dev_revisions = ['trunk'] + get_branches()[:2]
    stable = get_tagged_releases('stable')[0]
    oldstable = get_tagged_releases('oldstable')[0]

    config = {}

    config['revisions'] = revisions = []
    for r in dev_revisions:
        revisions.append({'revision': r, 'label': r +' (dev)'})
    revisions.append({'revision': stable, 'label': stable+' (stable)'})
    revisions.append({'revision': oldstable, 'label': oldstable+' (oldstable)'})
    for r in revisions:
        r['options'] = {'use_vnodes': True}
        r['java_home'] = "~/fab/jvms/jdk1.7.0_71"

    config['title'] = 'Jenkins C* regression suite - {}'.format(datetime.datetime.now().strftime("%Y-%m-%d"))

    return config

def test_simple_profile(cluster='sarang', load_rows=65E6, read_rows=65E6, threads=300):
    """Test the basic stress profile with default C* settings""" 
    config = create_baseline_config()
    config['cluster'] = cluster
    config['operations'] = [
        {'operation':'stress',
         'command': 'write n={load_rows} -rate threads={threads}'.format(**locals())},
        {'operation':'stress',
         'command': 'read n={read_rows} -rate threads={threads}'.format(**locals())},
        {'operation':'stress',
         'command': 'read n={read_rows} -rate threads={threads}'.format(**locals())}
    ]

    
    scheduler = Scheduler(CSTAR_SERVER)
    scheduler.schedule(config)
    

"""
Profiler
"""

import os
import logging
import textwrap
from fabric import api as fab
from fabric.tasks import execute

logging.basicConfig()
logger = logging.getLogger('profiler')
logger.setLevel(logging.INFO)

YOURKIT_DEFAULT_OPTIONS = ('delay=10000,onexit=memory,onexit=snapshot,sessionname=cassandra')
common_module = None


def set_common_module(module):
    global common_module
    common_module = module


def yourkit_is_enabled(revision_config=None):
    is_enabled = common_module.config.get('yourkit_profiler', False)
    if is_enabled and revision_config:
        if not revision_config.get('yourkit_profiler', False):
            is_enabled = False
        elif revision_config.get('product', 'cassandra') == 'dse':
            logger.info('Yourkit profiling is not compatible with DSE yet')
            is_enabled = False

    return is_enabled


def yourkit_get_config():
    agentpath = common_module.config.get('yourkit_agentpath', None)
    directory = common_module.config.get('yourkit_directory', None)
    options = common_module.config.get('yourkit_options', YOURKIT_DEFAULT_OPTIONS)
    if not (agentpath and directory):
        raise ValueError('Yourkit profiler requires yourkit_agentpath and yourkit_directory in the configuration.')

    return {'agentpath': agentpath, 'directory': directory, 'options': options}


@fab.parallel
def yourkit_clean():
    logger.info("Clean yourkit directory")
    config = yourkit_get_config()
    common_module.run_python_script(
        'utils',
        'clean_directory',
        '"{}"'.format(config['directory'])
    )


@fab.parallel
def yourkit_get_jvm_opts(profiling_type='sampling'):
    config = yourkit_get_config()
    config['type'] =  profiling_type

    jvm_opts=("\nJVM_OPTS=\"$JVM_OPTS -agentpath:{agentpath}={options},dir={directory},"
              "logdir={directory}/logs/\"\n").format(**config)

    trigger = 'StartCPUSampling'
    if profiling_type == 'tracing':
        trigger = 'StartCPUTracing'

    triggers = "JVMStartListener maxTriggerCount=-1\n    {}".format(trigger)
    fab.run('echo "{}" > ~/.yjp/triggers.txt'.format(triggers))

    return jvm_opts


@fab.parallel
def copy_yourkit(local_directory, rev_num):
    logger.info("Copying Yourkit data")
    config = yourkit_get_config()
    cfg = common_module.config['hosts'][fab.env.host]
    host_log_dir = os.path.join(local_directory, cfg['hostname'])
    os.makedirs(host_log_dir)
    fab.run("cd {} && tar -cvz --exclude=logs -f yourkit_revision_{}.tar.gz *".format(config['directory'], rev_num))
    fab.get(os.path.join(config['directory'], 'yourkit_revision_{}.tar.gz'.format(rev_num)), host_log_dir)

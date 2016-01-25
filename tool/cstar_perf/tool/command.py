import os
from fabric import api as fab
import sh
import tempfile
import shutil
import logging

logging.basicConfig()
logger = logging.getLogger('command')
logger.setLevel(logging.INFO)


class Command(object):
    """A command is an shell command executed as a cstar operation"""

    # Main executable
    command = None

    # Environment variables
    envs = {}

    # Define what params are invalid
    exclude_params = {}

    def __init__(self, params):
        self.artifacts = []
        self.params = params.split(' ')
        self.validate()
        self.directory = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.directory, 'artifacts'))

    def validate(self):
        """Validate the command parameters"""

        matched_params = [p for p in self.params if p in self.exclude_params]
        if matched_params:
            raise ValueError("Invalid parameter used with command '{}': {}".format(
                self.command, ','.join(matched_params)))

    def cmd(self, include_envs=True):
        """Return the command as string"""

        params = ' '.join(self.params)

        if include_envs:
            envs = []
            for k,v in self.envs.iteritems():
                envs.append("{}={}".format(k,v))

            return "{} {} {}".format(
                ' '.join(envs),
                self.command,
                params)
        else:
            return "{} {}".format(self.command, params)

    def run(self):
        """Run the actual command using fabric"""

        return fab.run('cd {}; {}'.format(self.directory, self.cmd()))


class Ctool(Command):
    """CTOOL command

    Ensure your cluster has an existing cluster setup with the name "cstar_perf" and that the
    cluster_config has automaton_path set.

    https://datastax.jira.com/browse/TESTINF-576

    """

    command = "ctool"
    exclude_params = ['launch', 'destroy']

    def __init__(self, params, config):
        super(Ctool,self).__init__(params)

        if 'automaton_path' not in config:
            raise ValueError('Cluster configuration requires "automaton_path" to use ctool command')

        self.envs['PATH'] = "{}:{}".format(os.path.join(config['automaton_path'], 'bin'), sh.PATH)
        self.envs['PYTHONPATH'] = config['automaton_path']

    def run(self):
        """ctool commands are run directly on the stress node"""

        cmd = self.cmd(include_envs=False)
        output = sh.bash('-c', "{}".format(cmd), _cwd=self.directory, _env=self.envs)
        logger.info(output)

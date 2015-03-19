import re
import yaml
from fabric import api as fab

name = 'cassandra'

MAX_CACHED_BUILDS = 10

# Git repositories
git_repos = [
    ('apache',        'git://github.com/apache/cassandra.git'),
    ('enigmacurry',   'git://github.com/EnigmaCurry/cassandra.git'),
    ('knifewine',     'git://github.com/knifewine/cassandra.git'),
    ('shawnkumar',    'git://github.com/shawnkumar/cassandra.git'),
    ('jbellis',       'git://github.com/jbellis/cassandra.git'),
    ('marcuse',       'git://github.com/krummas/cassandra.git'),
    ('pcmanus',       'git://github.com/pcmanus/cassandra.git'),
    ('belliottsmith', 'git://github.com/belliottsmith/cassandra.git'),
    ('bes',           'git://github.com/belliottsmith/cassandra.git'),
    ('iamaleksey',    'git://github.com/iamaleksey/cassandra.git'),
    ('tjake',         'git://github.com/tjake/cassandra.git'),
    ('carlyeks',      'git://github.com/carlyeks/cassandra.git'),
    ('aweisberg',     'git://github.com/aweisberg/cassandra.git'),
    ('mstump',        'git://github.com/mstump/cassandra.git'),
    ('snazy',         'git://github.com/snazy/cassandra.git'),
    ('blambov',       'git://github.com/blambov/cassandra.git')
]

def get_cassandra_path():
    return 'fab/cassandra'

def get_bin_path():
    return 'fab/cassandra/bin'

def get_cassandra_config_options(config):
    """Parse Cassandra's Config class to get all possible config values. 

    Unfortunately, some are hidden from the default cassandra.yaml file, so this appears the only way to do this."""

    # Get Jython helper : 
    jython_status = fab.run('test -f ~/fab/jython.jar', quiet=True)
    if jython_status.return_code > 0:
        fab.run("wget http://search.maven.org/remotecontent?filepath=org/python/jython-standalone/2.7-b1/jython-standalone-2.7-b1.jar -O ~/fab/jython.jar")

    pythonpath = "$HOME/fab/cassandra/build/classes/main"
    classpath = ":".join([pythonpath, "$HOME/fab/cassandra/lib/*", "$HOME/fab/jython.jar"])
    cmd = '{java_home}/bin/java -cp "{classpath}" -Dpython.path="{pythonpath}" org.python.util.jython -c "import org.apache.cassandra.config.Config as Config; print dict(Config.__dict__).keys()"'.format(java_home=config['java_home'], **locals())

    out = fab.run(cmd, combine_stderr=False)
    if out.failed:
        fab.abort('Failed to run Jython Config parser : ' + out.stderr)
    opts = yaml.load(out)
    p = re.compile("^[a-z][^A-Z]*$")
    return [o for o in opts if p.match(o)]

def bootstrap(config, git_fetch=True):
    """Install and configure Cassandra

    Returns the git id or the version checked out.
    """

    revision = config['revision']

    #Fetch latest git changes:
    if git_fetch:
        git_checkout_status = fab.run('test -d ~/fab/cassandra.git', quiet=True)
        if git_checkout_status.return_code > 0:
            fab.run('git init --bare ~/fab/cassandra.git')
            for name,url in git_repos:
                fab.run('git --git-dir=$HOME/fab/cassandra.git remote add {name} {url}'
                        .format(name=name, url=url), quiet=True)

        for name,url in reversed(git_repos):
            fab.run('git --git-dir=$HOME/fab/cassandra.git fetch {name}'.format(name=name))
            # TODO: What did this used to do? This used to be necessary,
            # but now it appears to delete branches: 
            # fab.run('git --git-dir=$HOME/fab/cassandra.git fetch -fup {name} +refs/*:refs/*'
            #         .format(name=name))

    fab.run('rm -rf ~/fab/cassandra')

    # Find the SHA for the revision requested:
    git_id = fab.run('git --git-dir=$HOME/fab/cassandra.git rev-parse {revision}'.format(revision=revision)).strip()
    # Check if we have already built the revision requested.
    # This speeds up consecutive runs of the same revision.
    fab.run('mkdir -p ~/fab/cassandra_builds')
    test_already_built = fab.run('test -d ~/fab/cassandra_builds/{git_id}'.format(git_id=git_id), quiet=True)
    if test_already_built.return_code == 0:
        # Copy previously built Cassandra
        fab.run('cp -a ~/fab/cassandra_builds/{git_id} ~/fab/cassandra'.format(git_id=git_id))
    else:
        # Build Cassandra
        # Checkout revision/tag:
        fab.run('mkdir ~/fab/cassandra')
        fab.run('git --git-dir=$HOME/fab/cassandra.git archive %s |'
                ' tar x -C ~/fab/cassandra' % revision)
        fab.run('echo -e \'%s\\n%s\\n%s\' > ~/fab/cassandra/0.GIT_REVISION.txt' % 
                (revision, git_id, config.get('log','')))

        fab.run('JAVA_HOME={java_home} ~/fab/ant/bin/ant -f ~/fab/cassandra/build.xml clean'.format(java_home=config['java_home']))
        if config['override_version'] is not None:
            fab.run('JAVA_HOME={java_home} ~/fab/ant/bin/ant -f ~/fab/cassandra/build.xml -Dversion={version}'.format(java_home=config['java_home'], version=config['override_version']))
        else:
            fab.run('JAVA_HOME={java_home} ~/fab/ant/bin/ant -f ~/fab/cassandra/build.xml'.format(java_home=config['java_home']))

        # Archive this build for future runs:
        fab.run('cp -a ~/fab/cassandra ~/fab/cassandra_builds/{git_id}'.format(git_id=git_id))

        # Remove old builds:
        num_builds = int(fab.run('ls -1 ~/fab/cassandra_builds | wc -l').strip())
        if num_builds > MAX_CACHED_BUILDS:
            fab.run('ls -t1 ~/fab/cassandra_builds | tail -n {num_to_delete} | xargs -iXX rm -rf ~/fab/cassandra_builds/XX'.format(
                num_to_delete=num_builds-MAX_CACHED_BUILDS))

def start(config):
    fab.puts("Starting Cassandra..")
    cmd = 'JAVA_HOME={java_home} nohup ~/fab/cassandra/bin/cassandra'.format(java_home=config['java_home'])
    fab.run(cmd)

def stop(clean):
    if clean:
        fab.run('pkill -f "java.*org.apache.*.CassandraDaemon"', quiet=True)
    else:
        fab.run('pkill -9 -f "java.*org.apache.*.CassandraDaemon"', quiet=True)

def is_running():
    pgrep = fab.run('pgrep -f "java.*org.apache.*.CassandraDaemon"', quiet=True)
    return True if pgrep.return_code == 0 else False

import sys
from setuptools import setup, find_packages

requires = (
    'fabric',
    'pyyaml',
    'sh',
    'fexpect'
)

setup(
    name = 'cstar_perf.tool',
    version = '1.0',
    description = 'Cassandra performance testing automation tools',
    author = 'The DataStax Cassandra Test Engineering Team',
    author_email = 'ryan@datastax.com',
    url = 'https://github.com/datastax/cstar_perf',
    install_requires = requires,
    namespace_packages = ['cstar_perf'],
    packages=find_packages(),
    zip_safe=False,
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    entry_points = {'console_scripts': 
                    ['cstar_perf_stress = cstar_perf.tool.stress_compare:main',
                     'cstar_perf_bootstrap = cstar_perf.tool.bootstrap:main',
                     'cstar_docker = cstar_perf.docker.cstar_docker:main']},
)


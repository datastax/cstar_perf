"""
Fabric file to setup a cluster so that fab_[cassandra|dse] can be used on it

Creates ~/.cstar_perf/cluster_config.json
Creates ~/fab directory on each node
"""

from fabric import api as fab

fab.env.use_ssh_config = True
fab.env.connection_attempts = 10

def setup_fab_dir():
    pass

def create_cluster_config(data_path="/data/cstar_perf"):
    cluster_config = {
        "block_devices": block_devices,
        "blockdev_readahead": blockdev_readahead,
        "hosts": {
            node.public_host_name: {
                "hostname": node.public_host_name,
                "internal_ip": node.private_host_name,
                "external_ip": node.public_ip,
                "seed": True,
                "datacenter": region_datacenters[node.region]
            } for node in cluster.nodes[1:]
        },
        "name": cluster_name,
        "stress_node": cluster.nodes[0].private_host_name,
        "user":"automaton",
        "data_file_directories": data_file_directories,
        "commitlog_directory": commitlog_directory,
        "saved_caches_directory": saved_caches_directory
    }

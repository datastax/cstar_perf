import os.path
import json

# Cluster configuration:
cluster_config_file = os.path.join(os.path.expanduser("~"), ".cstar_perf","cluster_config.json")

if os.path.exists(cluster_config_file):
    with open(cluster_config_file) as f:
        config = json.loads(f.read())
else:
    raise EnvironmentError, "No cluster config file found at {path}".format(path=cluster_config_file)


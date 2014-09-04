import os.path

EOF_MARKER = "$$$EOF$$$"
KEEPALIVE_MARKER = "$$$KEEPALIVE$$$"
CLIENT_CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".cstar_perf", "client.conf")
SERVER_CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".cstar_perf", "server.conf")
SERVER_KEY_PATH = os.path.join(os.path.expanduser("~"),'.cstar_perf','server.conf')

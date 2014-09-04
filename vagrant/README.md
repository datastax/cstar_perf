This directory contains a Vagrantfile to start a single node cluster
for use with stress_compare.py.

Setup
-----
Install sshd on the host machine, if you don't have it already:

    ubuntu/debian: sudo apt-get install ssh

Add this to your /etc/hosts file:

    # Vagrant cstar:
    192.168.56.201 cnode1

Add this to your ~/.ssh/config file:

    Host cnode*
        user vagrant
        StrictHostKeyChecking no
        UserKnownHostsFile=/dev/null

Bring the node up:

    vagrant up
    
    
If you have not yet setup a ~/fab directory on your host machine, you can rsync it from the vagrant node:

    rsync -av vagrant@cnode1:fab ~/

This should provision a new node and install all dependencies.

Copy cluster_config.json to home directory:

    mkdir -p ~/.cstar_perf
    cp cluster_config.json ~/.cstar_perf

Make sure you can ssh from your own machine to itself (fabric is going to do this for convenience):

    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys (adjust key filename as needed)

Now test it by running bootstrap.py from the parent directory, which should bring up Cassandra 2.1 on the node.

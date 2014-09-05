There are several different parts of cstar_perf that can be used
either as a whole, or as individual components (See
:doc:`architecture`.) This guide will walk through the installation of
cstar_perf.tool, which will be the core of what you need to start
benchmarking Cassandra. The next chapter of this guide will focus on
the :doc:`setup of cstar_perf.frontend` which sets up a full web-based
interface for scheduling tests, archiving results, and monitoring
multiple clusters.

*********************
Setup cstar_perf.tool
*********************

cstar_perf.tool is the core module of cstar_perf. It is what
bootstraps Cassandra and runs performance tests. You should install it
on a machine within the same network as your Cassandra cluster. It's
best to dedicate a machine to it, as it will be what runs
cassandra-stress and ideally should not have any resource contention
on it. If you don't have an extra machine, you can install it on the
same machine as one of your Cassandra nodes, just be aware of any
performance penalty you're introducing by doing so.

In this example, we have four computers::


                 +------> cnode1  
                 |      10.0.0.101
                 |                
     stress1 +----------> cnode2  
    10.0.0.100   |      10.0.0.102
                 |                
                 +------> cnode3  
                        10.0.0.103          


* ``stress1`` is the node hosting cstar_perf.tool.

* ``cnode1``, ``cnode2``, and ``cnode3`` are Cassandra nodes. These nodes have 4 SSDs for data storage, mounted at ``/mnt/d1``, ``/mnt/d2``, ``/mnt/d3``, and ``/mnt/d4`` 

Setting up your cluster
-----------------------

Key based SSH access
^^^^^^^^^^^^^^^^^^^^
 
The machine hosting cstar_perf.tool should have `key based SSH access`_ to the Cassandra cluster for both your regular user account as well as root. 

.. _key based SSH access: http://www.debian-administration.org/article/152/Password-less_logins_with_OpenSSH

In terms of our example, from your user account on ``stress1`` you
should be able to run ``ssh your_username@cnode1`` as well as ``ssh
root@cnode1`` without any password prompts.

When generating SSH keys, it works best if you don't specify a
password. You can use an SSH agent if you are uncomfortable doing
this, but be aware things will stop working when that agent isn't
running (system reboots, not logged in, etc.)

Software requirements
^^^^^^^^^^^^^^^^^^^^^

The machine running cstar_perf.tool needs to have the following packages installed:

* Python 2.7
* Python 2.7 development packages - (python-dev on debian)
* pip - (python-pip on debian)
* git

The Cassandra nodes also need to have the following:

* Python 2.7
* git

In addition, you need to prepare a ``~/fab`` directory to install on
each of your nodes. This will contain the JDK as well as a copy of
ant. Prepare this directory on the controller node (``stress1`` in our
example) and then rsync it to the others. Here's an example to set
this up on 64-bit Linux with Java 7u67 and ant 1.9.4 (links may
change, so modify accordingly.)::

    mkdir ~/fab
    cd ~/fab
    wget --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie;" http://download.oracle.com/otn-pub/java/jdk/7u67-b01/jdk-7u67-linux-x64.tar.gz
    tar xfv jdk-7u67-linux-x64.tar.gz
    rm jdk-7u67-linux-x64.tar.gz
    ln -s jdk1.7.0_67 java
    wget http://archive.apache.org/dist/ant/binaries/apache-ant-1.9.4-bin.tar.bz2
    tar xfv apache-ant-1.9.4-bin.tar.bz2
    rm apache-ant-1.9.4-bin.tar.bz2
    
The end result being that we can invoke java from ``~/fab/java/bin/java`` and ant from ``~/fab/ant/bin/ant``.

Copy this directory to each of your cassandra nodes::

    rsync -av ~/fab cnode1:
    rsync -av ~/fab cnode2:
    rsync -av ~/fab cnode3:

You'll know you got your SSH keys sorted out if copying those files didn't require you to enter any passwords. 


Cassandra Stress
^^^^^^^^^^^^^^^^

Additionally, on the node hosting cstar_perf.tool (``stress1`` in our
example) you need to download and build cassandra-stress. This is only
needs to be on the controller node (``stress1``)::

    mkdir ~/fab/stress
    cd ~/fab/stress
    git clone http://git-wip-us.apache.org/repos/asf/cassandra.git
    cd cassandra
    git checkout cassandra-2.1
    ~/fab/ant/bin/ant clean jar
    cd ..
    mv cassandra cassandra-2.1
    ln -s cassandra-2.1 default

The end result being that we can invoke cassandra-stress from ``~/fab/stress/default/tools/bin/cassandra-stress``. You'll know you have java and ant installed correctly if this build was successful.

Install cstar_perf.tool
^^^^^^^^^^^^^^^^^^^^^^^

Finally, you should install cstar_perf.tool onto your designated machine (``stress1`` in our example)::

    pip install cstar_perf.tool

Configuration
-------------

cstar_perf.tool needs to know about your cluster. For this you need to
create a JSON file located in ``~/.cstar_perf/cluster_config.json``.
Here's the config for our example cluster::

    {
        "commitlog_directory": "/mnt/d1/commitlog"
        "data_file_directories": [
            "/mnt/d2/data",
            "/mnt/d3/data",
            "/mnt/d4/data"
        ], 
        "block_devices": [
            "/dev/sdb",
            "/dev/sdc",
            "/dev/sdd",
            "/dev/sde"
        ], 
        "blockdev_readahead": "256", 
        "hosts": {
            "cnode1": {
                "internal_ip": "10.0.0.101",
                "hostname": "cnode1", 
                "seed": true
            },
            "cnode2": {
                "internal_ip": "10.0.0.102",
                "hostname": "cnode2", 
                "seed": true
            },
            "cnode3": {
                "internal_ip": "10.0.0.103",
                "hostname": "cnode3", 
                "seed": true
            },
        }, 
        "user": "your_username",
        "name": "example1", 
        "saved_caches_directory": "/mnt/d2/saved_caches"
    }

The required settings :

* **hosts** - all of your Cassandra nodes need to be listed here, including hostname and IP address.
* **name** - the name you want to give to this cluster.
* **block_devices** - The physical block devices that Cassandra is using to store data and commitlogs.
* **blockdev_readahead** - The default block device readhead setting for your drives (get it from running ``blockdev --getra /dev/DEVICE``)
* **user** - The user account that you use on the Cassandra nodes.

If you're familiar with Cassandra's ``cassandra.yaml``, you'll recognize the rest of these settings because they are from there. You can actually put more ``cassandra.yaml`` settings here if you know you'll *always* need them, but it's usually better to rely on the defaults and introduce different settings in your test scenarios, which you'll define later. 

Test cstar_perf_bootstrap
-------------------------

Now that cstar_perf.tool is installed and configured, you can bring up a test cluster to test that everything is working::

    cstar_perf_bootstrap apache/cassandra-2.1

This command will tell all of the cassandra nodes to download the latest Cassandra 2.1 from git, build it, and join a cluster together. You'll see a lot of text output showing you what the script is doing, but at the end of it all you should see something like::

    [10.0.0.101] All nodes available!
    INFO:benchmark:Started cassandra on 3 nodes with git SHA: bd396ec8acb74436fd84a9cf48542c49e08a17a6

Now that your cluster is automated, you can create some :doc:`test
definitions <running_tests>`, or setup the :doc:`web frontend <setup_cstar_perf_frontend>`.

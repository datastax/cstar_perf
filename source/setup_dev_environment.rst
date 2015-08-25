Setting up a cstar_perf development environment
===============================================

If you want to start hacking on cstar_perf, the best way to do that is
with the built-in ``cstar_docker`` command. This will setup a cluster,
client node, and frontend node automatically for you in separate
Docker containers. This means you can setup an entire cstar_perf
environment for development or testing, all on a single machine.

**NOTE: This tutorial is only good for development or demoing of
cstar_perf. Any real-world benchmarks should not be done from
within docker.**

Setup from scratch on Ubuntu 14.04
----------------------------------

This document will go through all the steps necessary to setup your
environment on a fresh Ubuntu 14.04 machine.

Install docker as per `the Docker instructions`_ ::

    wget -qO- https://get.docker.com/ | sh

.. _the Docker instructions: http://docs.docker.com/linux/step_one

    
Add your normal user account to the docker group::

    sudo usermod -aG docker $USER
    newgrp docker

Checkout cstar_perf source code someplace::

    export CSTAR_PERF_HOME=~/git/cstar_perf
    git clone https://github.com/datastax/cstar_perf $CSTAR_PERF_HOME

Create a Python virtual environment and install the code there::

    sudo apt-get install python-virtualenv
    virtualenv $CSTAR_PERF_HOME/env
    source $CSTAR_PERF_HOME/env/bin/activate
    pip install -e $CSTAR_PERF_HOME/tool
    
If you close your shell, or open a new one, remember to re-activate
the virtualenv. The rest of these instructions assume you are running
with the virtualenv activated::

    source $CSTAR_PERF_HOME/env/bin/activate

Build the cstar_perf docker base image::

    cstar_docker build

Launch a cluster to run the client and Cassandra nodes::

    cstar_docker launch test_cluster 1 -m

The above command launches a single node to act as a Cassandra
cluster, and an additional node for the cstar_perf client. For
development purposes, a single node Cassandra cluster is usually
suffient, but you can increase this if you are running on a powerful
box that can handle more than one node at a time. As another example,
if you specified 3 (instead of 1) you would get a total of 4 nodes, 1
to run the client, and 3 to run Cassandra.

The -m is useful for development, as your local cstar_perf checkout
($CSTAR_PERF_HOME in our example here) will be mounted inside the
docker containers directly, so any changes you make to the cstar_perf
source code will immediately be available in the docker containers as
well. (Without the -m, fresh cstar_perf code will be cloned from
http://github.com/datastax/cstar_perf)

Launch a node for the frontend::

    cstar_docker frontend test_frontend -m

If you are running on a machine with a webbrowser installed, the home
page of the frontend should automatically load to
http://localhost:8000. The frontend container is only bound to the
loopback device for security reasons. If you are running cstar_perf on
a headless box, you will need to create a port map on your own. One
way to do that is with ssh. From your personal machine, connect to the
machine running cstar_perf::

    ## Use this only if you need to access a remote cstar_perf instance:
    ssh your_cstar_perf_server -L 8000:localhost:8000 -N &

Then you should be able to load http://localhost:8000 normally.

At this point, the frontend can't run any tests because it still
doesn't know about the test cluster. To link the frontend to the test
cluster, use the associate command::

    cstar_docker associate test_frontend test_cluster

Now you are ready to run tests directly from the frontend web page.
Click Login in the upper right hand corner and enter the default
credentials:

* email: admin@example.com
* password: admin

To see what other commands cstar_docker includes, run it without any
arguments::

    cstar_docker

For example, you can ssh directly to any of the nodes, or start, stop,
or destroy them.

If you make useful modifications to cstar_perf, please consider
opening a pull-request on the `cstar_perf github page`_

.. _cstar_perf github page: https://github.com/datastax/cstar_perf/pulls

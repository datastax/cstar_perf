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

Setup from scratch on OSX 10.10+ (lower might work)
---------------------------------------------------
This document will go through all the steps necessary to setup your
environment on a fresh OSX machine.

Install VirtualBox_ and VirtualBox_Extension_Pack_

.. _VirtualBox: https://www.virtualbox.org/wiki/Downloads
.. _VirtualBox_Extension_Pack: https://www.virtualbox.org/wiki/Downloads

Install docker either from Docker_Toolbox_ or via Homebrew::

    brew install docker docker-machine docker-compose

.. _Docker_Toolbox: http://docs.docker.com/mac/step_one/

Create a docker base virtual machine -- adjusting the cpu and memory based on your plans and system::

    docker-machine create --driver virtualbox --virtualbox-cpu-count "4" --virtualbox-memory "6144" cstar-perf

Start the docker-machine image::

    docker-machine start cstar-perf

Setup environment vars for docker (must be run when starting a new shell::

    eval "$(docker-machine env cstar-perf)"

Setup routing to access docker containers:
Get the IP address of the docker-machine host::

    docker-machine inspect cstar-perf | grep "IPAddress"

Add route for docker containers (must be re-run after reboot / or add it to a startup script)::

    sudo route -n add 172.17.0.0/16 <<IP ADDRESS FROM ABOVE COMMAND>>

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

Setup cstar_perf code and build docker images
---------------------------------------------

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

-------

**If your base machine is Ubuntu:**

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

**If your base machine is OSX:**

You can load the frontend by it's IP address.  This can be found by::

    docker inspect test_frontend_00 | grep "IPAddress"

And visiting that IP on port 8000. ex: http://172.17.0.4:8000/

-------

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

-------

**OSX Users**

You can now edit code in your choice of editor and it will be updated
in the docker container.  If you make edits that need a restart to either
the client or server service, ssh in a use supervisord_.

server restart::

    cstar_docker ssh test_frontend
    sudo supervisorctl -c /supervisord.conf restart cstar_perf_server

client restart::

    cstar_docker ssh test_cluster
    sudo supervisorctl -c /supervisord.conf restart cstar_perf_client

-------

.. _supervisord: http://supervisord.org/

If you make useful modifications to cstar_perf, please consider
opening a pull-request on the `cstar_perf github page`_

.. _cstar_perf github page: https://github.com/datastax/cstar_perf/pulls

The rest of this guide is geared toward setting up a real-world test
environment using real hardware.

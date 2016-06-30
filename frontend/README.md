#Contents
 * [Deployment](#deployment):
  * [development](#development)
  * [production](#production)
 * [Generating keys](#generating-keys)
 * [Email Notifications](#email-notifications)


## Deployment
### Development

Requirements:

 * Python 2.7
 * python-dev
 * Virtualenv
 * Cassandra 2.0+ running on localhost

Ubuntu dependencies:

    sudo apt-get install python2.7 python2.7-dev python-virtualenv libtool cassandra curl

Setup a virtual environment:

    virtualenv --python=python2.7 env
    source env/bin/activate
    python setup.py develop

Make sure setup.py ends with the phrase "Finished processing
dependencies for cstar-perf-frontend", if you get an error about
`error: Could not find required distribution X`, I've found that
running setup.py again should fix it.

Ensure Cassandra is running on localhost.

Start the server:

    cstar_perf_server

cstar_perf_server is a wrapper for gunicorn, the server will only be accessible locally. Access the server at [http://localhost:8000](http://localhost:8000)


Start the notification server:

    cstar_perf_notifications

This is a zeromq notification server that will speed up the websocket
communication for sending job notifications. This is also required to
view the console output from the web frontend.

### Production

For production deployments, follow the same instructions above. You will want to use a process manager to run cstar_perf_server for you. There is a supervisor config file you can use to do this. TODO: document use of https://github.com/Supervisor/initscripts/blob/master/ubuntu to automate this on startup.

Setup nginx to proxy internet traffic to the cstar_perf_server:

    
## Generating Keys

Both the server and client require api keys to be able to communicate with eachother. The server key is generated automatically when you install the package, look for text similar to this on the console, or run `cstar_perf_server --get-credentials` :

    New server keys saved to /home/ryan/.cstar_perf_server
    Server public key: jmIe9LHKu+J4cQYqOdpmlu1TK2n3Euwwp6lGsyXMhXpa6ZM2ctoeanxe4/1ApaD/
    Server verify code: NzQ2YTBlYzgtY2U4Ny0xMWUzLWE4ZjMtZDRiZWQ5ODY0ZjRlfHA1azZhc1ZRa09NL1B6ekFiSS9tVGg2cC9UMkVsNXJBNDE4VmxScjJGaTdMbXorN09adHlwZTg2aTdma2g1dDQ=

The server keys are saved to ~/.cstar_perf/server.conf and will differ on each machine you deploy to.

To generate the client keys, run:

    cstar_perf_client --get-credentials

It will prompt you to input a name for your cluster, the server public key, which you retrieve using the preceding instructions above, as well as the server verify code. The verify code will validate that you copied the public key correctly.

Your new client public key will be printed on the screen. You need to import this key into the server's database. On the server, load your virtualenv and insert it:

    $ source env/bin/activate
    $ python
    Python 2.7.6 (default, Nov 26 2013, 12:52:49) 
    [GCC 4.8.2] on linux2
    Type "help", "copyright", "credits" or "license" for more information.
    >>> from cstar_perf_frontend.server.model import Model
    >>> db = Model()
    INFO:cstar_perf.model:Initializing Model...
    INFO:cstar_perf.model:Model initialized
    >>> db.add_cluster('YOUR_CLUSTER_NAME', CLUSTER_HOSTNAMES, 'YOUR_CLUSTER_DESCRIPTION')
    >>> db.add_pub_key('YOUR_CLUSTER_NAME', 'cluster', 'YOUR_CLIENT_PUBLIC_KEY')

Replace YOUR_CLUSTER_NAME with the same cluster name you gave above, CLUSTER_HOSTNAMES is a list containing the hostnames for the Cassandra nodes as defined in `cluster_config.json` and YOUR_CLIENT_PUBLIC_KEY with the generated client key that was printed on your screen.


## Email notifications

Test status updates can be sent to users via email as they happen. You
need to configure your SMTP settings in ~/.cstar_perf/server.conf. The
following is an appropriate configuration for using gmail servers:

    [server]
    url = http://cstar_perf.example.com

    [smtp]
    from=notifications@cstar_perf.example.com
    always_bcc=your_email@example.com
    server=smtp.gmail.com
    ssl=yes
    user=YOUR_USER@gmail.com
    pass=APP_SPECIFIC_PASSPHRASE

If you have a local SMTP server, you might be able to use this
minimal config:

    [server]
    url = http://cstar_perf.example.com

    [smtp]
    from=notifications@cstar_perf.example.com
    server=localhost

If there is no config found, email notifications will be disabled.

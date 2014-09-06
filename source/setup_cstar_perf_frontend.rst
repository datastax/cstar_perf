***********
Setup cstar_perf.frontend
***********

These instructions assume you are installing the frontend on a fresh
install of Ubuntu 14.04. 

Dependencies
============

* Python 2.7
* python-dev
* virtualenv
* nginx
* Cassandra 2.0+ running on localhost

Install dependencies::

    echo "deb http://www.apache.org/dist/cassandra/debian 20x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
    gpg --keyserver pgp.mit.edu --recv-keys 2B5C1B00
    gpg --export --armor 2B5C1B00 | sudo apt-key add -

    sudo add-apt-repository -y ppa:nginx/stable

    sudo apt-get update
    sudo apt-get install -y python2.7 python2.7-dev python-virtualenv libtool nginx cassandra


Google Authentication
=====================

The frontend uses Google authentication for user authentication. You
will need to generate an API key and copy the resulting
client_secrets.json file to ``~/.cstar_perf/client_secrets.json``

* Go to the `Google developers console`_
* Click on Create Project and give your new project a descriptive name.
* On the project's page, click 'Enable API'. On the following page,
  find and turn on 'Google+ API'. Although the API is called Google+,
  you and your users will not require a Google+ account, a regular
  Google account will be sufficient.
* On the left hand side, there's a link for Credentials. Click that
  and then click 'Create new Client ID'.
* In the popup it will have a box called 'Authorized Javascript
  origins', you need to put the full url to where you will host
  cstar_perf.frontend. The default 'Authorized redirect uri',
  '/oauth2callback' is correct.
* Download the client id and place it on the server at
  ``~/.cstar_perf/client_secrets.json``
* On the left hand side, click on 'Consent screen'. Enter your email
  address, a name for the app, and whatever other details you wish to
  provide.

.. _Google developers console: https://console.developers.google.com/


Install server
==============

Create a user account for the purpose of running cstar_perf.frontend.
All the commands listed here should be run from that account.

Create a virtualenv to keep the installation tidy::

    mkdir ~/app
    cd ~/app
    mkdir ~/logs
    virtualenv --python=python2.7 env
    source env/bin/activate
    
Install the frontend::

    pip install cstar_perf.frontend

To start the server for local testing::

    cstar_perf_server

This server will only be available at `http://localhost:8000`_


.. _http://localhost:8000: http://localhost:8000


Press ctrl-c to quit the server, we will install it more permanently
now.

You will need a nginx configuration file to proxy the server. You can
see the `gunicorn docs`_ for more information, or you can use the one
below.

Repalce ``automaton`` through this file with the name of the user
account you are using::

    worker_processes 1;
    user automaton automaton; 
    pid /home/automaton/app/nginx.pid;
    error_log /home/automaton/app/logs/nginx.error.log;
    
    events {
      worker_connections 1024; # increase if you have lots of clients
      accept_mutex off; # "on" if nginx worker_processes > 1
    }
    
    http {
      include mime.types;
      default_type application/octet-stream;    
      access_log /home/automaton/app/logs/nginx.access.log combined;
      sendfile on;
      tcp_nopush on; # off may be better for *some* Comet/long-poll stuff
      tcp_nodelay off; # on may be better for some Comet/long-poll stuff
    
      gzip on;
      gzip_http_version 1.0;
      gzip_proxied any;
      gzip_min_length 500;
      gzip_disable "MSIE [1-6]\.";
      gzip_types text/plain text/html text/xml text/css
                 text/comma-separated-values
                 text/javascript application/x-javascript
                 application/atom+xml;
    
      upstream app_server {    
        server 127.0.0.1:8000 fail_timeout=0;
      }
    
      map $http_upgrade $connection_upgrade {
          default upgrade;
          ''      close;
      }
    
      server {
        listen 80 default;
    
        client_max_body_size 4G;
        server_name _;
    
        keepalive_timeout 99999999999999;
    
        # path for static files
        root /path/to/app/current/public;
    
        location / {
          try_files $uri @proxy_to_app;
        }
    
        location /api/cluster_comms {
            proxy_pass http://localhost:8000;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $connection_upgrade;
        }
    
        location @proxy_to_app {
          proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
          proxy_set_header Host $http_host;    
          proxy_redirect off;
          proxy_pass http://app_server;
        }
      }
    }


.. _gunicorn docs: http://gunicorn-docs.readthedocs.org/en/19.1.1/deploy.html#nginx-configuration


Start nginx::

    sudo service nginx start

cstar_perf.frontend should at this point be reachable publically on
port 80.

Setup User Accounts
===================

There is no administration interface for adding users yet. You need to
insert accounts into the database directly. Here's a snippet to do
that from a python shell::

    $ source ~/app/env/bin/activate
    $ python
    Python 2.7.6 (default, Nov 26 2013, 12:52:49) 
    [GCC 4.8.2] on linux2
    Type "help", "copyright", "credits" or "license" for more information.
    >>> from cstar_perf.frontend.server.model import Model
    >>> db = Model()
    INFO:cstar_perf.model:Initializing Model...
    INFO:cstar_perf.model:Model initialized
    >>> admin = db.create_user('admin_user@gmail.com', 'Admin Full Name', ['user','admin'])
    >>> user = db.create_user('regular_user@gmail.com', 'User Full Name', ['user'])
    

Setup Clusters
==============

After you have setup :doc:`cstar_perf.tool <setup_cstar_perf_tool>` on your cluster, you can add it
the frontend.

On the server, you need to get your communication credentials, run::

    cstar_perf_server --get-credentials.

This will output a public key, and a verify code. Make note of these.

On the machine running cstar_perf.tool, install the frontend client::

    pip install cstar_perf.frontend

Get the client credentials::

    cstar_perf_client --get-credentials

The first time you run this it will ask you to give your cluster a
name, and ask for the server's public key and verify code. Enter the
same codes that the server output above. It will also output the
client's public key (text='Your public key is: xxxx'). Make not of
the client's public key.

There is no administration interface for adding clusters yet. You need
to insert clusters into the database directly. Here's a snippet to do
that from a python shell::

    $ source ~/app/env/bin/activate
    $ python
    Python 2.7.6 (default, Nov 26 2013, 12:52:49) 
    [GCC 4.8.2] on linux2
    Type "help", "copyright", "credits" or "license" for more information.
    >>> from cstar_perf.frontend.server.model import Model
    >>> db = Model()
    INFO:cstar_perf.model:Initializing Model...
    INFO:cstar_perf.model:Model initialized
    >>> db.add_cluster('YOUR_CLUSTER_NAME', NUMBER_OF_NODES, 'YOUR_CLUSTER_DESCRIPTION')
    >>> db.add_pub_key('YOUR_CLUSTER_NAME', 'cluster', 'YOUR_CLIENT_PUBLIC_KEY')


In the above example, ``YOUR_CLUSTER_NAME`` is the same name you chose
during the client installation, ``NUMBER_OF_NODES`` is how many nodes your
cluster has, and ``YOUR_CLIENT_PUBLIC_KEY`` is the client's public key
output from above.

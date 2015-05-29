# Use Python 2.7.8 due to gevent bug in later versions:
# https://github.com/gevent/gevent/issues/477
FROM python:2.7.8

RUN useradd -ms /bin/bash cstar
WORKDIR /home/cstar
ADD . /home/cstar/git/cstar_perf/
ADD client_secrets.json /home/cstar/.cstar_perf/client_secrets.json
RUN chown cstar:cstar -R /home/cstar

USER cstar
RUN virtualenv env
RUN /bin/bash -c "source /home/cstar/env/bin/activate && \
                  pip install -e /home/cstar/git/cstar_perf/frontend && \
                  cstar_perf_server --get-credentials && \
                  perl -pi -e 's/cassandra_hosts = localhost/cassandra_hosts = cassandra/' ~/.cstar_perf/server.conf"

CMD bash -c "source ~/env/bin/activate && \
             cstar_perf_notifications && \
             cstar_perf_server"

EXPOSE 8000

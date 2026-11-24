FROM postgres:18-alpine@sha256:54451ecb8ab38c24c3ec123f2fd501303a3a1856a5c66e98cecf2460d5e1e9d7

RUN apk add --no-cache python3 py3-pip py3-psycopg2 \
    && python3 -m venv --system-site-packages /opt/patroni \
    && /opt/patroni/bin/pip install --no-cache-dir "patroni[etcd3]==4.1.1"

COPY --chmod=0755 entrypoint.sh /usr/local/bin/nexus-patroni
COPY config.py /usr/local/lib/nexus-patroni-config.py
ENTRYPOINT ["/usr/local/bin/nexus-patroni"]

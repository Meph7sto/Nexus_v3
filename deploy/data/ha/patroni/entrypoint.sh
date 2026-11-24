#!/bin/sh
set -eu

install -d -o postgres -g postgres -m 0700 /run/nexus-ha/patroni
for item in ca.crt server.crt server.key; do
    install -o postgres -g postgres -m 0600 "/run/nexus-ha-input/$item" "/run/nexus-ha/patroni/$item"
done
install -d -o postgres -g postgres -m 0700 /var/lib/postgresql/18/docker
/opt/patroni/bin/python /usr/local/lib/nexus-patroni-config.py
privilege_drop=$(command -v gosu || command -v su-exec)
exec "$privilege_drop" postgres /opt/patroni/bin/patroni /run/nexus-ha/patroni/patroni.yml

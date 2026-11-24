import os
import pwd
from pathlib import Path

import yaml

directory = Path("/run/nexus-ha/patroni")
common = Path("/run/nexus-ha-common")
root_ca = str(directory / "ca.crt")
tls_cert = str(directory / "server.crt")
tls_key = str(directory / "server.key")
node_name = os.environ["HA_NODE_NAME"]
node_host = os.environ["HA_NODE_HOST"]
pg_mode = os.environ["HA_PG_MODE"]
if pg_mode not in {"sync_strict", "sync", "async"}:
    raise SystemExit("HA_PG_MODE must be sync_strict, sync or async")


def secret(name):
    text = (common / name).read_text().strip()
    if len(text) < 24 or any(ord(char) < 33 or ord(char) > 126 for char in text):
        raise SystemExit(f"Invalid secret file: {name}")
    return text


def auth(username, filename):
    return {
        "username": username,
        "password": secret(filename),
        "sslmode": "verify-full",
        "sslrootcert": root_ca,
    }


config = {
    "scope": os.environ["HA_SCOPE"],
    "namespace": "/nexus-ha/",
    "name": node_name,
    "restapi": {
        "listen": "0.0.0.0:8008",
        "connect_address": f"{node_host}:8008",
        "certfile": tls_cert,
        "keyfile": tls_key,
        "cafile": root_ca,
        "verify_client": "required",
        "authentication": {"username": "operator", "password": secret("rest-password")},
    },
    "ctl": {"cacert": root_ca, "certfile": tls_cert, "keyfile": tls_key},
    "etcd3": {
        "hosts": [f"{os.environ[f'HA_ETCD{i}_HOST']}:2379" for i in range(1, 4)],
        "protocol": "https",
        "cacert": root_ca,
        "cert": tls_cert,
        "key": tls_key,
    },
    "bootstrap": {
        "dcs": {
            "ttl": 30,
            "loop_wait": 10,
            "retry_timeout": 10,
            "primary_start_timeout": 0,
            "maximum_lag_on_failover": 1048576,
            "synchronous_mode": pg_mode != "async",
            "synchronous_mode_strict": pg_mode == "sync_strict",
            "synchronous_node_count": 1,
            "postgresql": {
                "use_pg_rewind": True,
                "use_slots": True,
                "parameters": {"wal_log_hints": "on", "password_encryption": "scram-sha-256"},
            },
        },
        "initdb": ["data-checksums", {"encoding": "UTF8"}],
        "pg_hba": [
            "local all postgres peer",
            "hostssl replication replicator 0.0.0.0/0 scram-sha-256",
            "hostssl replication replicator ::/0 scram-sha-256",
            "hostssl all all 0.0.0.0/0 scram-sha-256",
            "hostssl all all ::/0 scram-sha-256",
        ],
    },
    "postgresql": {
        "listen": "0.0.0.0:5432",
        "connect_address": f"{node_host}:5432",
        "data_dir": "/var/lib/postgresql/18/docker",
        "bin_dir": "/usr/local/bin",
        "authentication": {
            "superuser": auth("postgres", "postgres-password"),
            "replication": auth("replicator", "replicator-password"),
            "rewind": auth("rewind_user", "rewind-password"),
        },
        "use_unix_socket": True,
        "parameters": {
            "unix_socket_directories": "/var/run/postgresql",
            "ssl": "on",
            "ssl_cert_file": tls_cert,
            "ssl_key_file": tls_key,
            "max_connections": 50,
            "shared_buffers": "128MB",
            "effective_cache_size": "320MB",
            "maintenance_work_mem": "32MB",
            "work_mem": "2MB",
        },
    },
}
output = directory / "patroni.yml"
with open(output, "w", encoding="utf-8", opener=lambda path, flags: os.open(path, flags, 0o600)) as file:
    yaml.safe_dump(config, file)
postgres = pwd.getpwnam("postgres")
os.chown(output, postgres.pw_uid, postgres.pw_gid)

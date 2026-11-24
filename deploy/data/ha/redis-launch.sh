#!/bin/sh
set -eu
umask 077

read_password() {
    value=$(cat "$1")
    case "$value" in
        ''|*[!a-zA-Z0-9_-]*) echo "Redis secrets must be base64url characters" >&2; exit 1 ;;
    esac
    if [ "${#value}" -lt 32 ]; then
        echo "Redis secrets must contain at least 32 characters" >&2
        exit 1
    fi
    printf '%s' "$value"
}

validate_host() {
    case "$1" in
        ''|*[!a-zA-Z0-9.-]*) echo "Use a private IPv4 address or DNS hostname for Redis HA" >&2; exit 1 ;;
    esac
}

mode=$1
state=/data/redis.conf
if [ "$mode" = sentinel ]; then
    state=/data/sentinel.conf
elif [ "$mode" != primary ] && [ "$mode" != replica ]; then
    echo "Expected primary, replica, or sentinel" >&2
    exit 1
fi

if [ ! -f "$state" ]; then
    data_password=$(read_password /run/secrets/redis_data_password)
    announce=${HA_REDIS_ANNOUNCE_HOST:?set HA_REDIS_ANNOUNCE_HOST}
    primary=${HA_REDIS_INITIAL_MASTER_HOST:?set HA_REDIS_INITIAL_MASTER_HOST}
    validate_host "$announce"
    validate_host "$primary"
    if [ "$mode" = sentinel ]; then
        sentinel_password=$(read_password /run/secrets/redis_sentinel_password)
        if [ "$sentinel_password" = "$data_password" ]; then
            echo "Choose different Redis and Sentinel passwords" >&2
            exit 1
        fi
        {
            cat /etc/nexus/redis-sentinel.conf
            printf '\nrequirepass %s\n' "$sentinel_password"
            printf 'sentinel monitor nexus %s 6379 2\n' "$primary"
            printf 'sentinel auth-pass nexus %s\n' "$data_password"
            printf 'sentinel announce-ip %s\n' "$announce"
            printf 'sentinel announce-port 26379\n'
            printf 'sentinel down-after-milliseconds nexus 5000\n'
            printf 'sentinel failover-timeout nexus 30000\n'
            printf 'sentinel parallel-syncs nexus 1\n'
        } > "$state.tmp"
    else
        {
            cat /etc/nexus/redis.conf
            printf '\nrequirepass %s\nmasterauth %s\n' "$data_password" "$data_password"
            printf 'replica-announce-ip %s\nreplica-announce-port 6379\n' "$announce"
            if [ "$mode" = replica ]; then
                printf 'replicaof %s 6379\n' "$primary"
            fi
        } > "$state.tmp"
    fi
    mv "$state.tmp" "$state"
fi

if [ "$mode" = sentinel ]; then
    exec docker-entrypoint.sh redis-server "$state" --sentinel
fi
exec docker-entrypoint.sh redis-server "$state"

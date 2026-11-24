#!/bin/sh
set -eu

sample() {
    awk -v phase="$1" '
        /^VmRSS:/ { rss=$2 }
        /^VmHWM:/ { hwm=$2 }
        END { if (rss && hwm) print phase, rss, hwm }
    ' /proc/1/status
}

{
    sample idle
    i=0
    while [ ! -f /rehearsal/capacity-go ] && [ "$i" -lt 80 ]; do
        sleep 0.1
        i=$((i + 1))
    done
    i=0
    while [ -f /rehearsal/capacity-go ] && [ ! -f /rehearsal/capacity-done ] && [ "$i" -lt 80 ]; do
        sample active
        sleep 0.25
        i=$((i + 1))
    done
    sample end
} > /tmp/nexus-rehearsal-rss

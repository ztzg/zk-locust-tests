#!/usr/bin/env bash

set -e

ZK_LOCUST_TESTS="${BASH_SOURCE-$0}"
ZK_LOCUST_TESTS="$(dirname "${ZK_LOCUST_TESTS}")"
ZK_LOCUST_TESTS="$(cd "${ZK_LOCUST_TESTS}"; pwd)"

unset ZK_LOCUST_HOSTS
unset KAZOO_LOCUST_HOSTS
unset ZK_LOCUST_CLIENT

unset ZK_LOCUST_PSEUDO_ROOT
unset KAZOO_LOCUST_PSEUDO_ROOT

unset ZK_LOCUST_MIN_WAIT
unset ZK_LOCUST_MAX_WAIT

unset ZK_LOCUST_KEY_SIZE
unset ZK_LOCUST_VAL_SIZE

unset KAZOO_LOCUST_HANDLER
unset KAZOO_LOCUST_SASL_OPTIONS

unset ZK_LOCUST_ZK_METRICS_COLLECT
unset ZK_LOCUST_ZK_METRICS_CSV

unset LOCUST_EXTRA_STATS_CSV
unset LOCUST_EXTRA_STATS_DISTRIB
unset LOCUST_EXTRA_STATS_COLLECT

set_var() {
    local prefix="$1"; shift
    local key="$1"; shift
    local value="$1"; shift

    key="${key^^}"
    key="${key//-/_}"

    eval "export $prefix$key=${value@Q}"
}

dashdash=
multi_count=
multi_workdir=

while [ -z "$dashdash" -a "$#" -gt '0' ]; do
    case "$1" in
        --hosts|--client|--pseudo-root|--min-wait|--max-wait|--key-size|--val-size)
            set_var 'ZK_LOCUST_' "${1:2}" "$2"
            shift 2
            ;;
        --kazoo-handler|--kazoo-sasl-options)
            set_var 'KAZOO_LOCUST_' "${1:8}" "$2"
            shift 2
            ;;
        --zk-metrics-collect|--zk-metrics-csv)
            set_var 'ZK_LOCUST_' "${1:2}" "$2"
            shift 2
            ;;
        --stats-csv|--stats-distrib|--stats-collect)
            set_var 'LOCUST_EXTRA_' "${1:2}" "$2"
            shift 2
            ;;
        --multi)
            multi_count="$2"
            shift 2
            ;;
        --workdir)
            multi_workdir="$2"
            shift 2
            ;;
        --)
            dashdash="$1"
            shift
            ;;
        *)
            echo "Unrecognized argument '$1'; aborting.  (Tail: $*)" >&2
            exit 1
            ;;
    esac
done

if [ -z "$multi_count" ]; then
    exec locust "$@"
else
    if [ -z "$multi_workdir" ]; then
        multi_workdir="$(mktemp -d)"
        trap "rm -rf '$multi_workdir'" EXIT
    fi
    "$ZK_LOCUST_TESTS/multi-locust.sh" "$multi_count" "$multi_workdir" "$@"
fi

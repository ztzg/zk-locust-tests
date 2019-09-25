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

    declare -g "$prefix$key=$value"
    export "$prefix$key"
}

die() {
    echo "*** ERROR ***: $*" >&2
    exit 1
}

# Argument handling.

dashdash=
multi_count=
multi_workdir=
extra_locust_args=()
extra_report_args=()

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
        --bench-*)
            # This is an open-ended set of parameters which we are
            # consequently not clearing!
            set_var 'ZK_LOCUST_' "${1:2}" "$2"
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
        --report-dir)
            report_dir="$2"
            shift 2
            ;;
        --report-jobs)
            extra_report_args+=(-j "$2")
            shift 2
            ;;
        --)
            dashdash="$1"
            shift
            ;;
        *)
            die "Unrecognized argument '$1'; aborting.  (Tail: $*)"
            ;;
    esac
done

# Report directory setup.

if [ -n "$report_dir" ]; then
    if [ -d "$report_dir" ]; then
        die "Refusing to touch existing report directory '$report_dir'."
    fi

    mkdir -p "$report_dir"
    report_dir="$(cd "$report_dir"; pwd)"

    if [ -z "$ZK_LOCUST_ZK_METRICS_CSV" ]; then
        export ZK_LOCUST_ZK_METRICS_CSV="$report_dir/zk-metrics.csv"
    fi
    if [ -z "$LOCUST_EXTRA_STATS_CSV" ]; then
        export LOCUST_EXTRA_STATS_CSV="$report_dir/locust-stats.csv"
    fi
fi

# Locust invocation.

if [ -z "$multi_count" ]; then
    set +e
    locust "$@" "${extra_locust_args[@]}"
else
    if [ -z "$multi_workdir" ]; then
        multi_workdir="$(mktemp -d)"
        trap "rm -rf '$multi_workdir'" EXIT
    fi
    set +e
    "$ZK_LOCUST_TESTS/multi-locust.sh" "$multi_count" "$multi_workdir" \
        "$@" "${extra_locust_args[@]}"
fi

locust_status="$?"

set -e

# Report generation.

if [ -d "$report_dir" ]; then
    "$ZK_LOCUST_TESTS/report.py" \
        --metrics-dir "$report_dir" \
        --in-place \
        --zk-metrics-csv "$ZK_LOCUST_ZK_METRICS_CSV" \
        --stats-csv "$LOCUST_EXTRA_STATS_CSV" \
        "${extra_report_args[@]}"
fi

exit "$locust_status"

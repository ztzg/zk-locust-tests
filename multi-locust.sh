#!/usr/bin/env bash

set -e

COUNT="$1"; shift
WORKDIR="$1"; shift

pids=()

do_shutdown() {
    if [ "${#pids[@]}" -gt '0' ]; then
        kill "${pids[@]}" 2>/dev/null
    fi
}

trap 'do_shutdown' EXIT

common_args=()
master_args=()
slave_args=('--slave')

while [ "$#" -gt '0' ]; do
    case "$1" in
        -c|-r|-t)
            master_args+=("$1" "$2")
            shift 2
            ;;
        --no-web|--csv=*)
            master_args+=("$1")
            shift
            ;;
        *)
            common_args+=("$1")
            shift
    esac
done

if [ "$COUNT" -gt '0' ]; then
    master_args+=('--master' "--expect-slaves=$COUNT")

    for i in $(seq "$COUNT"); do
        nohup locust "${slave_args[@]}" "${common_args[@]}" \
              >"$WORKDIR/locust-slave-$i.log" &
        if [ "$?" = '0' ]; then
            pids+=("$!")
        fi
    done
fi

locust "${master_args[@]}" "${common_args[@]}"

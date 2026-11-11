#!/usr/bin/env bash
# Start seekdb inside the AMD container and wait until the observer is ready.
set -euo pipefail

RUN_DIR="${SEEKDB_RUN_DIR:-/var/lib/seekdb}"
BINARY="${SEEKDB_BINARY:-/seekdb/build_release/src/observer/seekdb}"

mkdir -p "${RUN_DIR}"

if [[ ! -x "${BINARY}" ]]; then
  echo "seekdb binary not found at ${BINARY}" >&2
  exit 1
fi

cd "${RUN_DIR}"
"${BINARY}" --port="${SEEKDB_PORT:-2881}" --nodaemon &
OBSERVER_PID=$!

echo "observer pid ${OBSERVER_PID}"

# wait for the observer to accept connections
for _ in $(seq 1 120); do
  if mysql -h 127.0.0.1 -P "${SEEKDB_PORT:-2881}" -uroot -e "SELECT VERSION()" >/dev/null 2>&1; then
    echo "observer ready"
    mysql -h 127.0.0.1 -P "${SEEKDB_PORT:-2881}" -uroot -e "SELECT VERSION()"
    wait "${OBSERVER_PID}"
    exit 0
  fi
  sleep 1
done

echo "the observer did not become ready in time" >&2
exit 1

#!/usr/bin/env bash
# Starts a hub and one conformance service per language, runs the interop driver, stops all.
#
#   LANGUAGES=go,rust,kotlin interop/run.sh
#
# Environment: LANGUAGES (default go,rust,kotlin), SNODRA_SERVER_VERSION (hub image, default 0.5.4),
# SNODRA_HUB_PORT (the hub's host port, default 7000; use 17000 on macOS, where the AirPlay
# Receiver holds 7000), LOCAL_SENDERS (languages whose library answers a unary send to a type it
# handles itself without the hub; default kotlin), GO_REF / GO_SRC and KOTLIN_REF / KOTLIN_SRC (see compose.yaml).
# Exit 0 only when every exchange passed.
set -euo pipefail

cd "$(dirname "$0")"
languages="${LANGUAGES:-go,rust,kotlin}"
port="${SNODRA_HUB_PORT:-7000}"
export SNODRA_HUB_PORT="$port"
services="${languages//,/ }"

cleanup() {
  docker compose down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

# shellcheck disable=SC2086 # the service names are a word list
docker compose up -d --build --wait hub $services

status=0
cargo run -q -p snodra-interop -- --hub "127.0.0.1:$port" --languages "$languages" --local-senders "${LOCAL_SENDERS-kotlin}" || status=$?
if ((status != 0)); then
  docker compose logs --no-color --tail 60 $services >&2 || true
fi
exit "$status"

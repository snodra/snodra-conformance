#!/usr/bin/env bash
# Starts a hub and one conformance service per language, runs the interop driver, stops all.
#
#   LANGUAGES=go,rust,kotlin,python interop/run.sh
#
# Environment: LANGUAGES (default go,rust,kotlin,python), SNODRA_SERVER_VERSION (hub image, default 0.6.0),
# SNODRA_HUB_PORT (the hub's host port, default 7000; use 17000 on macOS, where the AirPlay
# Receiver holds 7000), INTEROP_PROJECT (the compose project name; default snodra-interop-<pid>, so two
# runs on one host do not stop each other, but two runs need two SNODRA_HUB_PORT values),
# INTEROP_STRICT=1 (exit non-zero when a required exchange could not go through the hub),
# LOCAL_SENDERS (languages whose service answers a unary send to a type it
# handles itself without the hub, comma-separated; default none: every official service here sends
# through the hub), GO_REF / GO_SRC, KOTLIN_REF / KOTLIN_SRC and PYTHON_REF / PYTHON_SRC (see
# compose.yaml; PYTHON_REF defaults to v0.1.0, the released tag that carries the conformance service).
# Exit 0 only when every exchange it ran passed. The summary line names the required exchanges that
# were not exercised (those of the LOCAL_SENDERS); with INTEROP_STRICT=1 any such exchange is a failure.
set -euo pipefail

cd "$(dirname "$0")"
languages="${LANGUAGES:-go,rust,kotlin,python}"
port="${SNODRA_HUB_PORT:-7000}"
export SNODRA_HUB_PORT="$port"
services="${languages//,/ }"
project="${INTEROP_PROJECT:-snodra-interop-$$}"
compose() { docker compose -p "$project" "$@"; }
strict=()
if [[ "${INTEROP_STRICT:-}" == "1" ]]; then strict=(--strict); fi
local_senders=()
if [[ -n "${LOCAL_SENDERS:-}" ]]; then local_senders=(--local-senders "$LOCAL_SENDERS"); fi

cleanup() {
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

# shellcheck disable=SC2086 # the service names are a word list
compose up -d --build --wait hub $services

status=0
cargo run -q -p snodra-interop -- --hub "127.0.0.1:$port" --languages "$languages" ${local_senders[@]+"${local_senders[@]}"} ${strict[@]+"${strict[@]}"} || status=$?
if ((status != 0)); then
  compose logs --no-color --tail 60 $services >&2 || true
fi
exit "$status"

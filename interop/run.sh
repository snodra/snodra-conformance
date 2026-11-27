#!/usr/bin/env bash
# Starts a hub and one conformance service per language, runs the interop driver, stops all.
#
#   LANGUAGES=go,rust,kotlin,python interop/run.sh
#   SNODRA_HUB_PORT=17000 interop/run.sh --profile stores
#
# Environment: LANGUAGES (default go,rust,kotlin,python; the stores profile runs go,python),
# SNODRA_SERVER_VERSION (hub image, default 0.6.0), SNODRA_HUB_PORT (the hub's gRPC host port,
# default 7000; use 17000 on macOS, where the AirPlay Receiver holds 7000),
# SNODRA_HUB_HTTP_PORT (the dashboard HTTP host port, default one above SNODRA_HUB_PORT),
# INTEROP_PROJECT (the compose project name; default snodra-interop-<pid>, so two runs on one host
# do not stop each other, but two runs need two SNODRA_HUB_PORT values), INTEROP_STRICT=1 (exit
# non-zero when a required exchange or scenario could not be exercised), LOCAL_SENDERS (languages
# whose service answers a unary send to a type it handles itself without the hub, comma-separated;
# default none: every official service here sends through the hub), GO_REF / GO_SRC, KOTLIN_REF /
# KOTLIN_SRC and PYTHON_REF / PYTHON_SRC (see compose.yaml; GO_REF defaults to v0.6.0 and
# PYTHON_REF to v0.7.0, the released tags that carry the conformance and store-interop services).
# The stores profile also reads INTEROP_RUN (the unique token of the run; default generated) and
# INTEROP_HUB_WRITE_TOKEN (the dashboard write credential the control scenarios use; default
# snodra-interop-write-token). GO_SRC and PYTHON_SRC still override the released tags with local
# checkouts. The stores profile's --strict changes nothing because every scenario is required.
# Exit 0 only when every exchange it ran passed. The summary line names the required exchanges that
# were not exercised (those of the LOCAL_SENDERS); with INTEROP_STRICT=1 any such exchange is a failure.
set -euo pipefail

script="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
cd "$(dirname "$0")"
profile=""
while (($#)); do
  case "$1" in
    --profile)
      profile="${2:?--profile needs a value}"
      shift 2
      ;;
    --help | -h)
      # The whole header: every comment line from line 2 to the line before `set -euo pipefail`.
      awk 'NR == 1 { next } /^set -euo pipefail$/ { exit } { sub(/^# ?/, ""); print }' "$script"
      exit 0
      ;;
    *)
      echo "run.sh: unknown argument $1" >&2
      exit 2
      ;;
  esac
done
port="${SNODRA_HUB_PORT:-7000}"
export SNODRA_HUB_PORT="$port"
http_port="${SNODRA_HUB_HTTP_PORT:-$((port + 1))}"
export SNODRA_HUB_HTTP_PORT="$http_port"
export INTEROP_HUB_WRITE_TOKEN="${INTEROP_HUB_WRITE_TOKEN:-snodra-interop-write-token}"
project="${INTEROP_PROJECT:-snodra-interop-$$}"
compose() { docker compose -p "$project" "$@"; }
strict=()
if [[ "${INTEROP_STRICT:-}" == "1" ]]; then strict=(--strict); fi

# shellcheck disable=SC2329 # invoked by the EXIT trap below
cleanup() {
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

case "$profile" in
  "" | exchange | exchanges)
    languages="${LANGUAGES:-go,rust,kotlin,python}"
    services="${languages//,/ }"
    local_senders=()
    if [[ -n "${LOCAL_SENDERS:-}" ]]; then local_senders=(--local-senders "$LOCAL_SENDERS"); fi
    # shellcheck disable=SC2086 # the service names are a word list
    compose up -d --build --wait hub $services
    status=0
    cargo run -q -p snodra-interop -- --hub "127.0.0.1:$port" --languages "$languages" ${local_senders[@]+"${local_senders[@]}"} ${strict[@]+"${strict[@]}"} || status=$?
    if ((status != 0)); then
      # shellcheck disable=SC2086 # the service names are a word list
      compose logs --no-color --tail 60 $services >&2 || true
    fi
    exit "$status"
    ;;
  stores)
    languages="${LANGUAGES:-go,python}"
    if [[ "$languages" != "go,python" && "$languages" != "python,go" ]]; then
      echo "run.sh --profile stores: LANGUAGES must be go,python; the other languages' store-interop services are a follow-up" >&2
      exit 2
    fi
    run="${INTEROP_RUN:-$(date +%s)$$}"
    export INTEROP_RUN="$run"
    compose --profile stores up -d --build --wait hub store-go store-python
    status=0
    cargo run -q -p snodra-interop -- stores --hub "127.0.0.1:$port" --hub-api "127.0.0.1:$http_port" --languages "$languages" --run "$run" ${strict[@]+"${strict[@]}"} || status=$?
    if ((status != 0)); then
      compose logs --no-color --tail 100 store-go store-python kafka kafka-init rabbitmq rabbitmq-init postgres mysql mongo mongo-init >&2 || true
    fi
    exit "$status"
    ;;
  *)
    echo "run.sh: unknown profile ${profile} (the profiles are exchange and stores)" >&2
    exit 2
    ;;
esac

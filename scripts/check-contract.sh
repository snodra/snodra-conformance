#!/usr/bin/env bash
# The conformance contract (docs/contract.md) and its schemas (contract/*.json) must agree:
#   - contract/ holds at least one schema and every file is valid JSON;
#   - every `conformance.*` type named in a code span of the contract has a schema, and every schema is named;
#   - every relative link of the contract resolves.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
doc="$root/docs/contract.md"
dir="$root/contract"
fail=0

shopt -s nullglob
schemas=("$dir"/*.json)
if [ "${#schemas[@]}" -eq 0 ]; then
  echo "contract/ holds no schema" >&2
  exit 1
fi

for f in "${schemas[@]}"; do
  if ! jq empty "$f" 2>/dev/null; then
    echo "contract/$(basename "$f") is not valid JSON" >&2
    fail=1
  fi
done

# Types named in code spans: `conformance.Send`, `conformance.runner.Probe`, and fields of one such as
# `conformance.View.retry` (counted as the type). File names (`conformance.Send.json`) and the reserved namespace
# (`conformance.events.*`) are not types.
documented="$(grep -oE '`conformance\.[A-Za-z][A-Za-z0-9.]*`' "$doc" | tr -d '`' | grep -vE '\.(json|md)$' \
  | grep -E '^conformance\.(runner\.)?[A-Z]' | sed -E 's/^(conformance\.(runner\.)?[A-Z][A-Za-z0-9]*).*/\1/' | sort -u || true)"
available="$(for f in "${schemas[@]}"; do basename "$f" .json; done | sort -u)"

while IFS= read -r t; do
  [ -n "$t" ] || continue
  if ! grep -qxF "$t" <<<"$available"; then
    echo "type $t is in docs/contract.md but has no contract/$t.json" >&2
    fail=1
  fi
done <<<"$documented"

while IFS= read -r t; do
  [ -n "$t" ] || continue
  if ! grep -qxF "$t" <<<"$documented"; then
    echo "contract/$t.json exists but docs/contract.md never names type $t" >&2
    fail=1
  fi
done <<<"$available"

# Relative links: [text](target) whose target is not a URL or a bare anchor; the anchor part is not checked here.
while IFS= read -r link; do
  [ -n "$link" ] || continue
  path="${link%%#*}"
  [ -n "$path" ] || continue
  if [ ! -e "$(dirname "$doc")/$path" ]; then
    echo "docs/contract.md links to $link, which does not resolve" >&2
    fail=1
  fi
done < <(grep -oE '\]\([^)]+\)' "$doc" | sed -E 's/^\]\(//; s/\)$//' | grep -vE '^(https?:|mailto:|#)' || true)

[ "$fail" -eq 0 ] || exit 1
echo "contract: ${#schemas[@]} schemas, $(grep -c . <<<"$documented") types check out"

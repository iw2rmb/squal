#!/usr/bin/env bash
set -euo pipefail

targets=(
  core
  parser
  parserpg
  sql/runtime/pg
  sql/runtime/pg/cdc
  sql/reuse
  sql/graph
)

scan_targets=()
for target in "${targets[@]}"; do
  if [ -d "${target}" ]; then
    scan_targets+=("${target}")
  fi
done

if [ "${#scan_targets[@]}" -eq 0 ]; then
  echo "boundary check failed: no extracted package directories found"
  exit 1
fi

output="$(rg -n --glob '*.go' --glob '!**/*_test.go' "mill/internal" "${scan_targets[@]}" 2>&1)" || status=$?
status="${status:-0}"

if [ "${status}" -eq 0 ]; then
  echo "forbidden reference(s) to mill/internal found in shared modules:"
  echo "${output}"
  exit 1
fi

if [ "${status}" -eq 1 ]; then
  echo "boundary check passed: no references to mill/internal in extracted packages (${scan_targets[*]})"
  exit 0
fi

echo "boundary check failed to run ripgrep:"
echo "${output}"
exit "${status}"

#!/usr/bin/env bash
set -euo pipefail

output="$(rg -n "mill/internal" core parser 2>&1)" || status=$?
status="${status:-0}"

if [ "${status}" -eq 0 ]; then
  echo "forbidden reference(s) to mill/internal found in shared modules:"
  echo "${output}"
  exit 1
fi

if [ "${status}" -eq 1 ]; then
  echo "boundary check passed: no references to mill/internal in core/ or parser/"
  exit 0
fi

echo "boundary check failed to run ripgrep:"
echo "${output}"
exit "${status}"

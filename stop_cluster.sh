#!/usr/bin/env bash
set -euo pipefail

# Hosts to stop
HOSTS=(ridlserver04 ridlserver05 ridlserver11 ridlserver12)

# Remote repo path
REMOTE_DIR="/shared/MPR-Market-Implementation"

PARALLEL="${PARALLEL:-4}"

printf "%s\n" "${HOSTS[@]}" | \
  xargs -n1 -P"$PARALLEL" -I{} ssh {} "cd '$REMOTE_DIR' && docker compose down"


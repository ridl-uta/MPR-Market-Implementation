#!/usr/bin/env bash
set -euo pipefail

# Hosts
SERVER_HOST="local"
CLIENT_HOSTS=(ridlserver04 ridlserver05 ridlserver11 ridlserver12)

# Remote repo path
REMOTE_DIR="/shared/MPR-Market-Implementation"

# Server env
SUBSCRIBED_POWER="${SUBSCRIBED_POWER:-287000}"

# Client env
HPC_MANAGER_HOST="${HPC_MANAGER_HOST:-192.168.2.122}"
HPC_MANAGER_PORT="${HPC_MANAGER_PORT:-8000}"
HPC_MANAGER_FLASK_SERVER_PORT="${HPC_MANAGER_FLASK_SERVER_PORT:-5000}"
CLIENT_SCALE="${CLIENT_SCALE:-40}"
PARALLEL="${PARALLEL:-4}"

# Start server first (local)
if [[ "$SERVER_HOST" == "local" ]]; then
  SUBSCRIBED_POWER="$SUBSCRIBED_POWER" docker compose up -d --build --force-recreate server
else
  ssh "$SERVER_HOST" "cd '$REMOTE_DIR' && SUBSCRIBED_POWER='$SUBSCRIBED_POWER' docker compose up -d --build --force-recreate server"
fi

# Start clients on all hosts in parallel
printf "%s\n" "${CLIENT_HOSTS[@]}" | \
  xargs -n1 -P"$PARALLEL" -I{} ssh {} "cd '$REMOTE_DIR' && \
  HPC_MANAGER_HOST='$HPC_MANAGER_HOST' HPC_MANAGER_PORT='$HPC_MANAGER_PORT' HPC_MANAGER_FLASK_SERVER_PORT='$HPC_MANAGER_FLASK_SERVER_PORT' \
  docker compose up -d --build --force-recreate --no-deps --scale client=$CLIENT_SCALE client"

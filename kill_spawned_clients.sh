#!/usr/bin/env bash
set -euo pipefail

# Kill spawner and all spawned clients
pkill -f "spawn_clients.py" || true
pkill -f "Client/main.py" || true

# Show any remaining matching processes
pgrep -af "spawn_clients.py|Client/main.py" || true

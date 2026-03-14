#!/bin/bash
set -e

# Parse WAIT_FOR="mysql,kafka" into space-separated args for wait_for.py
SERVICES=$(echo "${WAIT_FOR:-kafka}" | tr ',' ' ')

python3 /app/wait_for.py $SERVICES

exec "$@"

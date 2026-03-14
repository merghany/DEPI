#!/bin/bash
set -e
python3 /app/wait_for.py $(echo "${WAIT_FOR:-kafka}" | tr ',' ' ')
exec "$@"

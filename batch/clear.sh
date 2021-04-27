#!/bin/bash
set -e
REDIS_CLI="redis-cli -h ${GAMEDB_HOST:-localhost} -p ${GAMEDB_PORT:-6379}"
$REDIS_CLI KEYS "*" | grep -v 'user_token' | xargs --no-run-if-empty $REDIS_CLI DEL
#!/usr/bin/env bash
set -euo pipefail

FILE="docker-compose-test.yaml"
PROFILE="${1:-all}"

usage() {
  echo "Uso: $0 [queues|exchange]"
  exit 1
}

case "$PROFILE" in
  queues)
    docker-compose -f "$FILE" down --remove-orphans -v || true
    docker-compose -f "$FILE" --profile queues up --build
    ;;
  exchange)
    docker-compose -f "$FILE" down --remove-orphans -v || true
    docker-compose -f "$FILE" --profile exchange up --build
    ;;
  *)
    usage
    ;;
esac

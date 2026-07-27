#!/bin/sh
set -eu

case "${1:-}" in
  alembic|pytest|ruff|mypy|python)
    exec "$@"
    ;;
  *)
    exec vk-collector "$@"
    ;;
esac

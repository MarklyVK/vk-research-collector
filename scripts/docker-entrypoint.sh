#!/bin/sh
set -eu

case "${1:-}" in
  alembic|pytest|ruff|mypy|python)
    exec "$@"
    ;;
  *)
    exec collector "$@"
    ;;
esac

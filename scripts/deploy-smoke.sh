#!/bin/sh
set -eu

docker compose config --quiet
docker compose build collector
docker compose up -d postgres
docker compose run --rm collector alembic upgrade head
docker compose run --rm collector --help >/dev/null
docker compose exec -T postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"
echo "Проверка развёртывания завершена успешно."

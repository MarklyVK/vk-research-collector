# Production runbook

## Ежедневная диагностика

```bash
cd /opt/vk-research-collector
export COLLECTOR_IMAGE=$(cat .deploy/current-image)
RUN_ID=$(sed -n 's/^COLLECTION_RUN_ID=//p' .env | tail -n 1)
docker compose -f compose.yaml -f compose.production.yaml ps
docker compose -f compose.yaml -f compose.production.yaml logs --tail=100 collector-worker
docker compose -f compose.yaml -f compose.production.yaml run --rm collector collection status \
  --run-id "$RUN_ID"
docker compose -f compose.yaml -f compose.production.yaml run --rm collector collection verify \
  --run-id "$RUN_ID"
cat .deploy/last-deployment.env
df -h /opt/vk-research-collector
```

Compose автоматически читает `.env`; shell-команда получает точный ID отдельно.

## Автоматический deploy

Push в `main` запускает повторный CI, сборку/push image на GitHub-hosted runner и
deployment на self-hosted runner. Predeploy backup:

```text
/opt/vk-research-collector/backups/predeploy-YYYYMMDD-HHMMSSZ-FULL_SHA.dump
```

Хранятся пять последних `predeploy-*`; manual, handoff и `server-before-handoff-*`
скрипт не ротирует. При 85% диска deploy прекращается до pull. При 95% дополнительно
останавливается worker. PostgreSQL остаётся запущенным.

## Ошибки

- Backup не прошёл `test -s`/`pg_restore --list`: исправить storage/PostgreSQL; миграции
  не начинались.
- Migration failure: PostgreSQL и backup доступны, worker остановлен; downgrade не
  запускать. Исправить forward migration или вручную восстановить backup.
- Health failure: проверить Actions summary и `.deploy/last-deployment.env`; image
  rollback уже предпринят, но несовместимость схемы требует ручного восстановления.
- Нет прогресса run: проверить token file, lease/retry и последние 100 строк worker.
- Старый volume найден: задать его точное имя как `POSTGRES_VOLUME_NAME`; не копировать
  данные в пустой volume вслепую.

Запрещены `docker compose down -v`, `docker volume rm`, автоматический Alembic
downgrade и одновременный local/server worker.

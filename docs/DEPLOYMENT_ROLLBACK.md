# Rollback production deployment

## Автоматический rollback image

Перед pull deploy сохраняет references в `.deploy/previous-image` и
`.deploy/current-image`. Если новый worker не запускается или healthcheck не проходит,
скрипт останавливает его, возвращает previous image и снова проверяет worker. Workflow
всё равно завершается красным статусом.

Если worker уже остановлен, но preflight падает до начала `alembic upgrade`, предыдущий
image также запускается автоматически. Deploy находит в том числе остановленный контейнер
и перед каждым production CLI-вызовом требует уже скачанный exact image. Это совместимо
со старой версией Compose на runner и исключает локальную сборку целевого тега.

Alembic downgrade автоматически не выполняется. Если новая миграция несовместима со
старым image, image rollback не считается восстановлением: оставьте worker
остановленным и восстановите predeploy backup вручную.

## Ручной rollback image

```bash
cd /opt/vk-research-collector
export COLLECTOR_IMAGE=$(cat .deploy/previous-image)
docker compose -f compose.yaml -f compose.production.yaml stop collector-worker
printf '%s\n' "$COLLECTOR_IMAGE" > .deploy/current-image
docker compose -f compose.yaml -f compose.production.yaml up -d collector-worker
docker compose -f compose.yaml -f compose.production.yaml ps
docker compose -f compose.yaml -f compose.production.yaml logs --tail=100 collector-worker
```

## Восстановление PostgreSQL

Это отдельная аварийная операция с потерей изменений после backup. Сначала остановите
worker и сохраните дополнительный manual dump. Затем подтвердите выбранный файл:

```bash
cd /opt/vk-research-collector
export COLLECTOR_IMAGE=$(cat .deploy/current-image)
docker compose -f compose.yaml -f compose.production.yaml stop collector-worker
docker compose -f compose.yaml -f compose.production.yaml exec -T postgres \
  pg_dump -U vk_collector -d vk_research -Fc > backups/manual-before-restore-$(date -u +%Y%m%d-%H%M%SZ).dump
docker compose -f compose.yaml -f compose.production.yaml exec -T postgres \
  pg_restore --list < backups/predeploy-SELECTED.dump >/dev/null
```

После ручного подтверждения администратора пересоздайте `vk_research`, восстановите
`predeploy-SELECTED.dump`, выполните `alembic upgrade head`, `classification summary`,
`collection status --run-id ...` и `collection verify --run-id ...`. Запускайте worker
только после успешной сверки. Volume не удаляется ни на одном шаге.

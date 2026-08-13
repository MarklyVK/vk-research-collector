# Production runbook

## Ежедневная диагностика

Workflow `Production collection control` каждый час проверяет очередь. Если разрешённый
run ещё активен, scheduled-запуск завершается без изменений. Если партия подписок
завершена, workflow повторно проходит Gate A и создаёт следующую cohort. Ручной запуск
`start-subscriptions` по-прежнему требует подтверждение `START_SUBSCRIPTIONS`.

Деплой и ручная очистка не удаляют backup, указанный в `verified_backup` незавершённого
запуска. После изменения прав каталога деплой восстанавливает для UID collector только
`traverse` на каталог и read-only ACL на такие dump-файлы. Если Pilot A целиком состоит
из уже недоступных пользователей, control ограниченно пробует следующие cohort (до трёх),
не ослабляя capacity gate.
Старый paused-run не возобновляется, если после его создания уже завершилась более
новая партия подписок: в этом случае строится свежий cohort без повторной обработки.

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

## Telegram monitor

Health check выполняется каждые пять минут, daily report — в `09:00 Europe/Moscow`.
Проверка timers:

```bash
uid=$(id -u deploy)
sudo -u deploy env XDG_RUNTIME_DIR="/run/user/$uid" systemctl --user status \
  vk-collector-telegram-health.timer vk-collector-telegram-daily.timer
sudo -u deploy env XDG_RUNTIME_DIR="/run/user/$uid" systemctl --user list-timers --all
```

Ручной безопасный test alert и daily report:

```bash
sudo -u deploy /usr/bin/python3 scripts/telegram-monitor.py --test-alert
sudo -u deploy /usr/bin/python3 scripts/telegram-monitor.py --daily
```

Первичная настройка выполняется только после создания бота через официальный
`@BotFather`: `sudo ./scripts/setup-telegram-monitor.sh`. Подробности, thresholds,
dedup/recovery и журнал — в [`TELEGRAM_MONITORING.md`](TELEGRAM_MONITORING.md).

## Автоматический deploy

Push в `main` запускает повторный CI, сборку/push image на GitHub-hosted runner и
deployment на self-hosted runner. Predeploy backup:

```text
/opt/vk-research-collector/backups/predeploy-YYYYMMDD-HHMMSSZ-FULL_SHA.dump
```

Хранится последний проверенный `predeploy-*`; manual, handoff и
`server-before-handoff-*` скрипт не ротирует. При 85% диска deploy прекращается до
pull. После backup и pull пороги проверяются повторно. При 95% дополнительно
останавливается worker. PostgreSQL остаётся запущенным.

Deployment также проверяет versioned Telegram unit-файлы, обновляет user units
`deploy`, выполняет `daemon-reload` и оставляет оба timer active/enabled. Для работы
после перезагрузки один раз должен быть включён `loginctl enable-linger deploy`.

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

## Deploy migration 0005 и expansion food_service

Deploy обязан создать и проверить predeploy backup до `alembic upgrade head`.
Migration не удаляет и не переименовывает прежние labels; автоматический downgrade при
наличии `food_service` запрещён. После deploy проверить:

```bash
docker compose -f compose.yaml -f compose.production.yaml run --rm collector alembic current
docker compose -f compose.yaml -f compose.production.yaml run --rm collector alembic check
docker compose -f compose.yaml -f compose.production.yaml run --rm collector \
  classification summary --subject food_service
docker compose -f compose.yaml -f compose.production.yaml run --rm collector \
  collection status --run-id 9be2813e-e1de-4ac9-bc07-7d92ac82438c
```

Deploy не создаёт search или incremental run автоматически. Их разрешает оператор
только после полной reclassification, импорта, независимого аудита и capacity gate.

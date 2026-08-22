# Одноразовый перенос PostgreSQL на сервер

> Никогда не запускайте локальный и серверный worker одновременно.

Поток: остановка local worker → custom dump → checksum → scp → server backup →
clean restore → Alembic → verify → server worker.

## Экспорт на Windows

Из корня локального репозитория:

```powershell
.\scripts\export-server-handoff.ps1 `
  -ServerUser deploy `
  -ServerHost <SERVER_HOST> `
  -RemoteDirectory /opt/vk-research-collector/backups `
  -RunId 9be2813e-e1de-4ac9-bc07-7d92ac82438c
```

Скрипт останавливает `collector-worker`, проверяет остановку, сохраняет status и
classification counts, создаёт `server-handoff-*.dump` и manifest, проверяет архив
через `pg_restore --list`, считает SHA256 и передаёт оба файла. Dump не проходит
через GitHub Artifacts/Packages. Локальный worker не запускается автоматически даже
после успешной передачи.

Без передачи на сервер опустите `-ServerUser` и `-ServerHost`; файлы останутся в
локальном `backups/`, исключённом из Git.

## Импорт на Debian 12

Убедитесь, что `compose.yaml`, `compose.production.yaml`, `.env`, token file и
`.deploy/current-image` подготовлены. Команда восстановления:

```bash
cd /opt/vk-research-collector
sudo -u deploy ./scripts/import-server-handoff.sh \
  backups/server-handoff-YYYYMMDD-HHMMSSZ-RUN_ID.dump \
  backups/server-handoff-YYYYMMDD-HHMMSSZ-RUN_ID.manifest.json \
  --confirm-replace-database
```

Флаг обязателен только для непустой серверной БД, но в production runbook он указан
явно. Скрипт:

1. сверяет basename и SHA256 с manifest, затем выполняет `pg_restore --list`;
2. останавливает server worker и получает общий lock с deployment;
3. создаёт `server-before-handoff-*.dump`, если серверная БД непуста;
4. завершает подключения, пересоздаёт только указанную БД и восстанавливает dump;
5. выполняет `alembic upgrade head` и `alembic check`;
6. сверяет approved/rejected/pending, run, failed/rejected jobs и дубли;
7. только после всех проверок запускает server worker и проверяет прогресс;
8. пишет `.deploy/handoff-*.report.txt`.

При любом несовпадении новый worker не запускается. Backup и исходный handoff dump
не удаляются.

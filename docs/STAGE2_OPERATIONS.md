# Эксплуатация второго этапа

## Безопасный порядок

```bash
docker compose up -d postgres
make backup PURPOSE=stage2-migration
docker compose run --rm collector alembic upgrade head
make collection-plan
make collection-pilot
make collection-status
```

Перед full run изучите `docs/STAGE2_PILOT_REPORT.md` и убедитесь, что capacity gate
имеет решение `passed`. Затем `make collection-plan APPLY=1` и `make collection-run`.
Subscriptions остаются выключенными до отдельного решения.

После уменьшенного повторного pilot команда ниже снимет capacity-паузу только если
машиночитаемый отчёт содержит `decision=passed` и прогноз не выше safe limit:

```bash
make collection-capacity-apply RUN_ID=FULL_RUN_ID
make collection-run RUN_ID=FULL_RUN_ID
```

Pause/resume:

```bash
docker compose run --rm collector collection pause --run-id RUN_ID
docker compose run --rm collector collection resume --run-id RUN_ID
docker compose run --rm collector collection run --run-id RUN_ID --until-idle
```

Диагностика: `collection status`, `collection verify`, `collection summary`,
`docker compose logs --tail=200`. При 85% диска не создавайте тяжёлые задания; при 95%
оставьте run на паузе. Никогда не очищайте PostgreSQL автоматически.

## Автономный worker

После успешного `capacity-apply` запустите service, который сам находит последний
разрешённый full run и хранит весь progress в PostgreSQL:

```bash
docker compose up -d collector-worker
docker compose ps
docker compose logs -f collector-worker
docker compose run --rm collector collection status --run-id RUN_ID
```

Service имеет `restart: unless-stopped`. Проверенный production run:
`9be2813e-e1de-4ac9-bc07-7d92ac82438c`. Штатная остановка завершает текущие page
transactions и прекращает захват новых jobs; повторный `docker compose up -d
collector-worker` продолжает тот же run. На Windows автоматическое продолжение после
перезагрузки возможно только после запуска Docker Desktop. На Debian включите Docker:

```bash
sudo systemctl enable --now docker
docker compose up -d postgres collector-worker
```

Точные команды управления:

```bash
docker compose run --rm collector collection status --run-id RUN_ID
docker compose run --rm collector collection summary
docker compose run --rm collector collection pause --run-id RUN_ID
docker compose run --rm collector collection resume --run-id RUN_ID
```

## Privacy and data minimization

Доступны `privacy inspect-user VK_ID`, `privacy delete-user VK_ID --confirm` и
`privacy inspect-group VK_ID`. Перед удалением создайте backup. Команды не печатают
контакты или secrets; read-only роль используется для аналитики. Backup хранится с
ограниченными правами. Retention errors/logs настраивается операционно, основные данные
не удаляются по TTL.

## Backup

Формат: `backups/stage2-PURPOSE-YYYYMMDD-HHMMSSZ.dump`. После `pg_dump -Fc` обязательно
проверить ненулевой размер и `pg_restore --list`. Каталог исключён из Git.

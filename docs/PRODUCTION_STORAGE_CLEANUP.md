# Очистка production storage

Ручной workflow `Production storage cleanup` запускается только из `main` на production
runner и использует общий deployment lock. Сначала обязателен режим `preview`. Режим
`apply` дополнительно требует строку `DELETE_OLD_BACKUPS`.

Перед очисткой скрипт проверяет:

- Alembic `20260810_0007`;
- `running/healthy` у PostgreSQL и worker;
- совпадение worker image с `.deploy/current-image`;
- последний PostgreSQL dump через `pg_restore --list`.

Сохраняются:

- последний проверенный PostgreSQL dump;
- текущий immutable collector image и один rollback image;
- PostgreSQL volume и все прочие Docker volumes;
- `exports`, `.env`, `secrets`, Git checkout и файлы мониторинга.

Удаляются только после preview:

- остальные файлы внутри `/opt/vk-research-collector/backups`;
- старые collector image references, кроме current/rollback;
- dangling images и неиспользуемый Docker build cache;
- остановленные одноразовые контейнеры Compose-проекта;
- старые `.deploy/*.tmp.*`.

Скрипт не содержит `docker system prune`, `docker volume rm`, `compose down -v` или
рекурсивного удаления production-каталога. После apply он повторно проверяет health,
текущий/rollback image и сохранённый dump, а в GitHub summary пишет занятое место до и
после очистки.


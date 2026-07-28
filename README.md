# VK Research Collector

Первый этап классифицировал группы, второй реализует безопасный возобновляемый сбор
публичных данных только по approved-группам. PostgreSQL хранит очередь, lease,
checkpoints, посты, метаданные вложений, memberships, минимальные профили и публичные
подписки. Subscriptions по умолчанию выключены до отдельного capacity gate.

## Локальный запуск

```bash
cp .env.example .env
mkdir -p secrets exports
touch secrets/vk_tokens.txt
docker compose up -d postgres
docker compose run --rm collector alembic upgrade head
make search-groups
```

## Второй этап

```bash
make backup PURPOSE=before-stage2
make migrate
make collection-plan
make collection-pilot
make collection-status
```

`collection plan` ничего не изменяет без `--apply`. Полный run запрещён до успешного
pilot capacity gate. Возобновление: `make collection-resume RUN_ID=...`, затем
`make collection-run RUN_ID=...`. Проверка: `make collection-verify RUN_ID=...`.

Privacy-команды показывают только агрегаты: `collector privacy inspect-user VK_ID` и
`collector privacy inspect-group VK_ID`. Удаление пользователя требует явного
`privacy delete-user VK_ID --confirm` и предварительного backup.

Частоту запросов, concurrency и лимиты сущностей меняют через `.env`, пересборка образа
не нужна. Команды перечислены в `Makefile`; stage 2 описан в
`docs/STAGE2_OPERATIONS.md`, эксплуатация сервера — в `docs/OPERATIONS_DEBIAN12.md`.

## Внимание: порт PostgreSQL

> **Публикация `0.0.0.0:5432` небезопасна.** Production deploy делает это только по явному решению владельца. Обязательно ограничьте порт firewall. Рекомендуемый вариант — `POSTGRES_BIND_ADDRESS=127.0.0.1` и SSH tunnel: `ssh -L 15432:127.0.0.1:5432 user@server`.

Секреты хранятся только в `.env` и `secrets/vk_tokens.txt`; эти пути исключены из Docker build context и Git.

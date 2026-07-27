# VK Research Collector

Первый этап: поиск групп VK, сохранение кандидатов в PostgreSQL, экспорт и импорт ручной классификации. Сбор постов, подписчиков и пользователей пока не запускается.

## Локальный запуск

```bash
cp .env.example .env
mkdir -p secrets exports
touch secrets/vk_tokens.txt
docker compose up -d postgres
docker compose run --rm collector alembic upgrade head
make search-groups
```

Частоту запросов и concurrency меняют через `.env`, пересборка образа не нужна. Команды перечислены в `Makefile`; эксплуатация сервера — в [docs/OPERATIONS_DEBIAN12.md](docs/OPERATIONS_DEBIAN12.md).

## Внимание: порт PostgreSQL

> **Публикация `0.0.0.0:5432` небезопасна.** Production deploy делает это только по явному решению владельца. Обязательно ограничьте порт firewall. Рекомендуемый вариант — `POSTGRES_BIND_ADDRESS=127.0.0.1` и SSH tunnel: `ssh -L 15432:127.0.0.1:5432 user@server`.

Секреты хранятся только в `.env` и `secrets/vk_tokens.txt`; эти пути исключены из Docker build context и Git.

# Эксплуатация на Debian 12

Сервер рассчитан на 1 CPU, 1 GB RAM, диск 10 GB и swap 1 GB. Установите Docker Engine с Compose plugin, `git`, `curl`, затем клонируйте проект в `/opt/vk-research-collector`.

## Первичная настройка

```bash
cd /opt/vk-research-collector
sudo sh scripts/create-swap.sh
cp .env.example .env
install -d -m 700 secrets
install -m 600 /dev/null secrets/vk_tokens.txt
chmod 600 .env
```

Задайте в `.env` отдельные случайные `POSTGRES_PASSWORD` и `POSTGRES_READER_PASSWORD`. В `secrets/vk_tokens.txt` положите один VK-токен на строку. Не коммитьте эти файлы.

```bash
docker compose up -d postgres
docker compose run --rm collector alembic upgrade head
docker compose run --rm collector --help
```

Read-only роль создаётся при первой инициализации тома PostgreSQL. После миграций её default privileges позволяют читать новые таблицы. При изменении пароля на существующем томе выполните `ALTER ROLE` вручную от пользователя приложения.

## PostgreSQL и безопасное подключение

По умолчанию пример `.env` использует `POSTGRES_BIND_ADDRESS=127.0.0.1`. Подключение с рабочей станции:

```bash
ssh -L 15432:127.0.0.1:5432 SERVER_USER@SERVER_HOST
psql 'postgresql://vk_reader:PASSWORD@127.0.0.1:15432/vk_research'
```

> **ОПАСНО:** значение `POSTGRES_BIND_ADDRESS=0.0.0.0` публикует PostgreSQL в интернет. Оно оставлено в production deploy по явному решению владельца. Ограничьте TCP/5432 firewall-правилом до доверенных IP, используйте стойкий пароль и как можно скорее вернитесь к `127.0.0.1` с SSH tunnel.

Настройки `shared_buffers=128MB`, `work_mem=4MB`, `maintenance_work_mem=64MB`, `max_connections=20` и лимиты контейнеров подобраны для слабого сервера. Docker-логи ограничены пятью файлами по 10 MB.

## Диск и обслуживание

`scripts/disk-guard.sh` при 85% удаляет только старые файлы `/tmp` и старые ротируемые логи, затем уведомляет Telegram. Данные PostgreSQL он не трогает. При 95% создаётся `/var/lib/vk-research-collector/disk-stop`; планировщик тяжёлых задач должен проверять этот файл.

Пример root cron каждые 10 минут:

```cron
*/10 * * * * set -a; . /opt/vk-research-collector/.env; set +a; /opt/vk-research-collector/scripts/disk-guard.sh
```

Диагностика и резервная копия:

```bash
docker compose ps
docker compose logs --tail=200
docker compose exec -T postgres pg_dump -U vk_collector vk_research | gzip > vk_research.sql.gz
```

## GitHub Actions

Variables: `SERVER_HOST`, `SERVER_PORT=22`, `SERVER_USER`, `DEPLOY_PATH=/opt/vk-research-collector`, `POSTGRES_DB=vk_research`, `POSTGRES_USER=vk_collector`, `POSTGRES_PORT=5432`.

Secrets: `DEPLOY_SSH_PRIVATE_KEY`, `POSTGRES_PASSWORD`, `POSTGRES_READER_PASSWORD`, `VK_TOKENS_B64`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`. `VK_TOKENS_B64` — base64 от файла с одним токеном на строку. CI использует тестовую PostgreSQL и не требует настоящих VK-токенов.

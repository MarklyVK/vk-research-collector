# GitHub Actions и production deployment

## Потоки

`.github/workflows/ci.yml` запускается для pull request в `main`, push в `main`,
`feat/**`, `fix/**`, `chore/**` и вручную. Все jobs работают на `ubuntu-latest`:

- `quality`: Ruff, format, mypy, unit tests и secret scan;
- `postgres-integration`: временный PostgreSQL 16, Alembic, integration suite и fake VK;
- `compose-smoke`: отдельный Compose project, Docker build, миграции и тесты в image.

Временный Compose project удаляется вместе с его тестовым volume. Production volume
этот workflow не видит.

`.github/workflows/deploy-production.yml` запускается только при push в `main` или
ручном `workflow_dispatch` на `main`. `quality` и `build-image` используют
`ubuntu-latest`. Образ публикуется как:

```text
ghcr.io/marklyvk/vk-research-collector/collector
```

Tags: `sha-<полный commit SHA>`, `main`, `latest`. Deployment всегда получает только
неизменяемый SHA-tag. `deploy` — единственный job на
`[self-hosted, linux, x64, production, vk-collector]`; production verify выполняется
внутри него. Отдельный GitHub-hosted `verify` фиксирует безопасный отчёт. Группа
concurrency `production-deployment` и серверный `flock` исключают параллельный deploy.

Все сторонние actions закреплены на полных commit SHA. Build args и workflow secrets
не содержат runtime-секреты.

## GitHub Environment

В репозитории откройте `Settings → Environments → New environment → production`.
В deployment branches/tags выберите только `main`. Для полностью автоматического
deploy не добавляйте required reviewers. Self-hosted runner должен быть привязан
только к приватному `MarklyVK/vk-research-collector`.

GitHub environment не нуждается в VK-токенах или PostgreSQL-паролях. Они остаются на
сервере:

```text
/opt/vk-research-collector/.env
/opt/vk-research-collector/secrets/vk_tokens.txt
```

Оба файла имеют mode `600`; workflow их не создаёт и не перезаписывает.

## Self-hosted runner

Получите одноразовый registration token через `Settings → Actions → Runners → New
self-hosted runner`, затем из checkout репозитория выполните:

```bash
chmod +x scripts/install-github-runner.sh
RUNNER_TOKEN='ОДНОРАЗОВЫЙ_TOKEN' sudo -E \
  REPOSITORY_URL='https://github.com/MarklyVK/vk-research-collector' \
  RUNNER_USER=deploy \
  RUNNER_HOME=/opt/vk-research-collector/runner \
  ./scripts/install-github-runner.sh
sudo systemctl --no-pager status 'actions.runner*'
```

Скрипт использует проверенный SHA256 архив GitHub Actions Runner 2.336.0. Token не
записывается на диск и удаляется из переменной после регистрации. Runner работает
как systemd service и использует только исходящие HTTPS-соединения.

Доступ пользователя `deploy` к Docker фактически равен высоким системным
привилегиям. Поэтому этот runner нельзя подключать к публичному репозиторию,
pull-request jobs или непроверенным workflow.

## Что делает deploy

1. Повторно подтверждает, что exact SHA принадлежит `main`.
2. Входит в GHCR с job-scoped `GITHUB_TOKEN` только на чтение.
3. Проверяет пользователя, файлы, mode, Compose, volume, PostgreSQL и диск.
4. Атомарно синхронизирует только Compose, keywords и разрешённые scripts.
5. Создаёт и проверяет `pg_dump -Fc`; оставляет пять последних predeploy backup.
6. Скачивает SHA-image, останавливает только worker, выполняет Alembic upgrade/check.
7. Запускает PostgreSQL и worker без `down`, проверяет health, данные и прогресс run.
8. Пишет `.deploy/last-deployment.env` и GitHub Actions summary.

Dry-run:

```bash
sudo -u deploy bash scripts/deploy-production.sh --dry-run \
  --source-dir "$PWD" \
  --deploy-dir /opt/vk-research-collector \
  --image ghcr.io/marklyvk/vk-research-collector/collector:sha-<FULL_SHA> \
  --git-sha <FULL_SHA>
```

Dry-run не синхронизирует файлы, не создаёт backup, не останавливает worker, не
применяет миграции и не перезапускает сервисы.

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
неизменяемый SHA-tag и digest, возвращённый `docker/build-push-action`. После pull
скрипт сверяет digest и OCI label `org.opencontainers.image.revision`. `deploy` —
единственный job на
`[self-hosted, linux, x64, production, vk-collector]`; production verify выполняется
внутри него. Отдельный GitHub-hosted `verify` фиксирует безопасный отчёт. Группа
concurrency `production-deployment` и серверный `flock` исключают параллельный deploy.

GitHub-hosted `notify-failure` запускается через `always()`, если `quality`,
`build-image`, `deploy` или `verify` не завершились успешно. Он получает
`TELEGRAM_BOT_TOKEN` и `TELEGRAM_CHAT_ID` только из Environment `production`;
self-hosted runner эти secrets не получает. Безопасная настройка через stdin описана
в [`TELEGRAM_MONITORING.md`](TELEGRAM_MONITORING.md).

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

Оба файла имеют mode `600`; workflow их не создаёт и не перезаписывает. Token file
принадлежит UID/GID контейнера `10001:10001`. Каталог `secrets` разрешает группе
`vkcollector` только traversal, поэтому пользователь `deploy` может проверить наличие
и mode файла, но не прочитать его. Каталог `exports` доступен и UID 10001, и группе
`vkcollector`.

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
4. Проверяет отсутствие tracked-изменений и обновляет production checkout только
   fast-forward до exact SHA из GitHub checkout; untracked `.env`, `secrets` и
   `runner/` не затрагиваются.
5. Создаёт и проверяет `pg_dump -Fc`; на сервере 8,4 GB оставляет только последний
   predeploy backup, а manual/handoff backups не ротирует.
6. Скачивает SHA-image, сверяет digest/revision и останавливает только worker с
   graceful timeout 360 секунд.
7. Выполняет Alembic upgrade/check из заранее проверенного exact image и запускает только worker с `--no-deps --no-build`;
   PostgreSQL container и volume не пересоздаются.
8. Пишет `.deploy/last-deployment.env` и GitHub Actions summary.

Dry-run:

```bash
sudo -u deploy bash scripts/deploy-production.sh --dry-run \
  --source-dir "$PWD" \
  --deploy-dir /opt/vk-research-collector \
  --image ghcr.io/marklyvk/vk-research-collector/collector:sha-<FULL_SHA> \
  --image-digest sha256:<64_HEX> \
  --git-sha <FULL_SHA>
```

Dry-run не синхронизирует файлы, не создаёт backup, не останавливает worker, не
применяет миграции и не перезапускает сервисы.

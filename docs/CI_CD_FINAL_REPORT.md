# Итоговый отчёт CI/CD

Дата проверки: 30.07.2026. Ветка: `feat/approved-data-collection`.

## 1–7. Workflows и image

Созданы `.github/workflows/ci.yml` и `.github/workflows/deploy-production.yml`; старый
SSH workflow удалён. CI запускается для PR в `main`, push в `main`, `feat/**`,
`fix/**`, `chore/**` и вручную. Production запускается только для push в `main` или
`workflow_dispatch` на `main`; exact SHA повторно проверяется как достижимый из main.

GitHub-hosted `ubuntu-latest` используют CI jobs `quality`, `postgres-integration`,
`compose-smoke` и production jobs `quality`, `build-image`, `verify`. Только job
`deploy` использует `[self-hosted, linux, x64, production, vk-collector]`.

Image: `ghcr.io/marklyvk/vk-research-collector/collector`. Tags:
`sha-<полный SHA>`, `main`, `latest`; deploy получает SHA-tag и опубликованный digest,
после pull сверяет их с OCI revision. Параллельность блокируют GitHub concurrency
`production-deployment` и `flock` на сервере.

## 8–12. Secrets, backup, migrations, health и rollback

Secrets находятся только в `/opt/vk-research-collector/.env` и
`/opt/vk-research-collector/secrets/vk_tokens.txt`, mode `600`. Workflow не передаёт и
не перезаписывает их.

Перед миграциями создаётся
`backups/predeploy-YYYYMMDD-HHMMSSZ-FULL_SHA.dump` командой `pg_dump -Fc`, затем
обязательны `test -s` и `pg_restore --list`. Ротируются только predeploy backups;
последние пять сохраняются.

Worker останавливается отдельно с graceful timeout 360 секунд. Выполняются
`alembic current`, `alembic upgrade head` и `alembic check`; downgrade отсутствует.
Запускается только worker с `--no-deps --no-build`, поэтому production PostgreSQL
container не пересоздаётся. Затем проверяются container state/health,
CLI summaries, run status, failed/rejected jobs, дубли, диск и прогресс. Summary
содержит SHA, images, backup, revision, states, run counters, DB size, disk и duration.

При health failure возвращается `.deploy/previous-image`, но workflow остаётся
failed. При migration failure worker не запускается и image rollback не имитирует
откат схемы; PostgreSQL и backup остаются доступны для ручного восстановления.

## 13–15. Handoff и граница автоматизации

`export-server-handoff.ps1` останавливает local worker, создаёт/проверяет custom dump,
SHA256 и manifest, при необходимости выполняет scp и не перезапускает worker.
`import-server-handoff.sh` проверяет checksum/archive, блокирует deploy, делает backup
непустой server DB, требует `--confirm-replace-database`, восстанавливает чистую БД,
применяет Alembic, сверяет данные и только затем запускает server worker.

Ручными остаются: увеличение диска до 20 GB, Docker, пользователь `deploy`, runtime
secrets, первоначальный GHCR pull, runner registration, Environment `production` и
первый database handoff. После bootstrap push в `main` полностью автоматизирует CI,
build/push image, backup, migration, restart, verify, report и image rollback.

## 16. Результаты проверок

```text
ruff check .                                      OK
ruff format --check .                             OK (74 files)
mypy src                                          OK (27 source files)
pytest -q                                         25 passed, 8 skipped
Linux deployment contract suite                  10 passed
docker compose config --quiet                    OK
production merged Compose config                 OK
docker compose build                             OK
alembic upgrade head в collector image           OK
pytest -q в collector image                      23 passed, 10 skipped
actionlint 1.7.7                                 OK
ShellCheck 0.10.0                                OK
```

Skipped-тесты — PostgreSQL suites вне соответствующей среды и contract-тесты внутри
минимального runtime image; оба набора отдельно выполнены в предназначенных Linux/DB
окружениях.

## 17. Commits

```text
71de1b6 ci: add isolated GitHub Actions quality workflow
288d6ae ci: publish collector images to GHCR
26eac9b ci: add production self-hosted deployment
f80cf23 ops: add safe production deployment scripts
cfea2b9 ops: add database handoff automation
<этот commit> docs: add GitHub deployment and server runbooks
```

Push не выполнялся.

## 18. Точная команда регистрации runner

```bash
chmod +x scripts/install-github-runner.sh
RUNNER_TOKEN='ОДНОРАЗОВЫЙ_TOKEN' sudo -E \
  REPOSITORY_URL='https://github.com/MarklyVK/vk-research-collector' \
  RUNNER_USER=deploy \
  RUNNER_HOME=/opt/vk-research-collector/runner \
  ./scripts/install-github-runner.sh
sudo systemctl --no-pager status 'actions.runner*'
```

## 19. Точная команда экспорта

```powershell
.\scripts\export-server-handoff.ps1 `
  -ServerUser deploy `
  -ServerHost 1.2.3.4 `
  -RemoteDirectory /opt/vk-research-collector/backups `
  -RunId 9be2813e-e1de-4ac9-bc07-7d92ac82438c
```

## 20. Точная команда восстановления

```bash
cd /opt/vk-research-collector
sudo -u deploy ./scripts/import-server-handoff.sh \
  backups/server-handoff-YYYYMMDD-HHMMSSZ-RUN_ID.dump \
  backups/server-handoff-YYYYMMDD-HHMMSSZ-RUN_ID.manifest.json \
  --confirm-replace-database
```

## 21. Первый production deployment

1. Выполнить `docs/SERVER_BOOTSTRAP.md` до создания stable volume и ручного pull
   первого SHA-image.
2. Зарегистрировать repository-scoped runner и создать Environment `production` с
   разрешением только для `main`.
3. Запустить Windows export: он остановит local worker и передаст dump/manifest.
4. Запустить server import с явным флагом; дождаться verify и запуска server worker.
5. Убедиться, что local worker остался остановлен, а `.deploy/handoff-*.report.txt`
   имеет `status=success`.
6. Перезапустить production workflow для exact commit из `main` или сделать следующий
   push в `main`.
7. Проверить GitHub summary и `/opt/vk-research-collector/.deploy/last-deployment.env`.

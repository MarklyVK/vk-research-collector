# Telegram-мониторинг production

## Архитектура

Монитор — короткий Python-процесс на стандартной библиотеке. Он запускается от
`deploy`, не имеет собственного daemon и не монтирует Docker socket в контейнер.
Read-only состояние собирается через Docker CLI, `psql` внутри PostgreSQL container,
`systemctl`, `/proc/meminfo`, `df` и versioned deployment report.

Два user-level systemd timer пользователя `deploy`:

- `vk-collector-telegram-health.timer` — через две минуты после загрузки и затем
  каждые пять минут;
- `vk-collector-telegram-daily.timer` — `09:00 Europe/Moscow`, независимо от timezone
  сервера.

User manager работает после перезагрузки благодаря `loginctl enable-linger deploy`.
Unit-файлы находятся в `deploy/systemd` и обновляются deployment-скриптом. State:

```text
/opt/vk-research-collector/.deploy/telegram-monitor/state.json
```

State записывается атомарно с mode `600`, не содержит token, `.env` или VK secrets.
Первый alert отправляется сразу, неизменившийся повторяется не чаще раза в три часа,
изменение severity отправляется сразу. После устранения приходит отдельный
`RECOVERED`. Неотправленные сообщения остаются pending и повторяются следующим timer.
Daily report отправляется не более одного раза за московскую дату.

Отдельный GitHub-hosted job уведомляет о `failure`, `cancelled` или неожиданном
`skipped` любого production job. Self-hosted runner не получает Telegram token из
GitHub Environment.

## Первичная безопасная настройка

Telegram не позволяет создать бота через API. Один раз:

1. Откройте официальный `@BotFather`.
2. Отправьте `/newbot`.
3. Задайте имя, например `VK Collector Monitor`.
4. Задайте уникальный username с окончанием `bot`.
5. Откройте созданного бота и отправьте `/start`.
6. На сервере выполните:

```bash
cd /opt/vk-research-collector
sudo ./scripts/setup-telegram-monitor.sh
```

Скрипт читает token через `read -rsp`, проверяет `getMe`, показывает только безопасные
chat metadata, записывает token в:

```text
/opt/vk-research-collector/secrets/telegram_bot_token.txt
```

Token file получает owner `deploy` и mode `600`. В `.env` сохраняются только
`TELEGRAM_ENABLED=true`, путь к token file и числовой chat ID; прежний inline
`TELEGRAM_BOT_TOKEN` очищается. Token не передаётся аргументом процесса, не пишется во
временный файл и удаляется из shell variable. В конце отправляется setup test.

Проверка без записи:

```bash
sudo ./scripts/setup-telegram-monitor.sh --dry-run
```

Для GitHub Environment `production` значения передаются только через stdin:

```bash
sudo -u deploy sh -c 'cat /opt/vk-research-collector/secrets/telegram_bot_token.txt' |
  gh secret set TELEGRAM_BOT_TOKEN --env production

sudo -u deploy sh -c \
  "sed -n 's/^TELEGRAM_CHAT_ID=//p' /opt/vk-research-collector/.env | tail -n 1" |
  gh secret set TELEGRAM_CHAT_ID --env production
```

Shell tracing должен быть выключен. Значения нельзя печатать или передавать через
`--body`.

## Контролируемые проблемы

- отсутствие, остановка, unhealthy, restart loop и OOM PostgreSQL/worker;
- `failed`, `cancelled`, `paused*`, `error_message`, stale lease, отсутствие progress,
  retry spike и несколько активных run;
- отсутствие/недоступность VK token file внутри worker, auth/token errors и длительный
  rate-limit;
- дубли posts/memberships/subscriptions и jobs по rejected-группам;
- PostgreSQL connection, Alembic head и резкий рост БД;
- диск `85%` warning, `95%` critical, менее `1 GiB` свободного места;
- доступная RAM менее `100 MiB`, swap от `90%`;
- inactive/disabled runner или отсутствие listener PID;
- failed deployment report, Git/OCI/report revision mismatch и неподтверждённый backup;
- failure/cancelled/skipped production workflow jobs.

Active `running` jobs сами по себе не считаются ошибкой `collection verify`. Stalled
определяется по сохранённым completed/API counters с учётом run status, pending,
worker health, lease, `retry_wait` и будущего `next_attempt_at`.

## Команды оператора

Чтобы обращаться к user manager `deploy`:

```bash
uid=$(id -u deploy)
sudo -u deploy env XDG_RUNTIME_DIR="/run/user/$uid" \
  systemctl --user status vk-collector-telegram-health.timer
sudo -u deploy env XDG_RUNTIME_DIR="/run/user/$uid" \
  systemctl --user status vk-collector-telegram-daily.timer
sudo -u deploy env XDG_RUNTIME_DIR="/run/user/$uid" \
  systemctl --user list-timers --all
```

Ручные проверки:

```bash
sudo -u deploy /usr/bin/python3 \
  /opt/vk-research-collector/scripts/telegram-monitor.py --health --dry-run

sudo -u deploy /usr/bin/python3 \
  /opt/vk-research-collector/scripts/telegram-monitor.py --test-alert

sudo -u deploy /usr/bin/python3 \
  /opt/vk-research-collector/scripts/telegram-monitor.py --daily
```

Журнал:

```bash
uid=$(id -u deploy)
sudo -u deploy env XDG_RUNTIME_DIR="/run/user/$uid" \
  journalctl --user -u vk-collector-telegram-health.service -n 200 --no-pager
```

Отключение и повторное включение:

```bash
uid=$(id -u deploy)
sudo -u deploy env XDG_RUNTIME_DIR="/run/user/$uid" systemctl --user disable --now \
  vk-collector-telegram-health.timer vk-collector-telegram-daily.timer
sudo -u deploy env XDG_RUNTIME_DIR="/run/user/$uid" systemctl --user enable --now \
  vk-collector-telegram-health.timer vk-collector-telegram-daily.timer
```

Сбой Telegram никогда не останавливает PostgreSQL, worker, collection run или уже
успешно запущенные production-сервисы.

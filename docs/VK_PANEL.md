# VK-панель (исследование виральности)

Выключена по умолчанию: сервис `panel-worker` в профиле `panel`, деплой его не запускает.

## Запуск (только после «да» руководителя)

```bash
# 1. зафиксировать выборку (status=draft, сбор не идёт)
docker compose ... run --rm collector panel freeze --name virality-2026-11 \
  --selection-csv /app/exports/vk_panel/panel_selection_20261109.csv --seed 20261109 \
  --config-json '{"interim_export_at":"2026-11-12T18:59:00+00:00","stop_at":"2026-11-14T03:00:00+00:00"}'
# 2. активировать (started_at = сейчас)
docker compose ... run --rm collector panel activate --name virality-2026-11
# 3. цикл: тик каждые 10 мин + ретроспектива порциями по 5 сообществ между тиками
docker compose ... --profile panel up -d panel-worker
# сводка / ручная выгрузка
docker compose ... run --rm collector panel status --name virality-2026-11
docker compose ... run --rm collector panel export --name virality-2026-11 --label manual
```

## Что делает тик

1. `wall.get` (`poll_count`=30 последних постов) у сообществ, которым пора по `poll_interval_seconds`
   (по умолчанию раз в час), — 10 сообществ в одном `execute`.
2. `wall.getById` по постам с наступившей отметкой возраста (1, 3, 6, 12, 24, 48, 72 ч) — 100 постов
   в вызове, 10 вызовов в `execute`; ответ облегчается в VKScript (`@.`). Допуск: max(20 мин, 10 % T);
   опоздавший замер сохраняется без отметки (`target_age_h` NULL), отметка считается пропущенной.
   Всегда пишется фактический возраст (`age_seconds`).
3. Раз в сутки — `groups.getById` (`members_count`).
4. `interim_export_at` — Parquet в `exports/vk_panel/interim/<UTC>/`; `stop_at` — финальная выгрузка
   `exports/vk_panel/final/<UTC>/` и заморозка (`status=frozen`).

Ретроспектива: все посты за `backfill_days` (90) + один снимок счётчиков.
Одна страница (100 постов) на `execute`; при ошибке VK 13 («response size is too big») `count`
уменьшается 100 → 50 → 20. Сообщество, упавшее 3 раза подряд, уходит в конец очереди
(лог `panel_backfill_deferred`); прогресс внутри сообщества хранится в памяти worker'а.
Интервал тиков считается от начала тика, поэтому ретроспектива не сдвигает замеры.

Ретроспектива 12 месяцев (решение 09.11): после того как 90 дней готовы у всех сообществ,
`ext_backfill` идёт дальше по стене до `ext_backfill_since` (11.11.2025) и сохраняет только посты
с собственным видео/клипом (origin `ext12m`) и один снимок счётчиков; медиа в очередь не ставятся.
Не больше `ext_backfill_daily_pages` (6000) страниц в сутки МСК — запас лимита для живой панели;
общий бюджет 80 % действует. Готовность — `panel_communities.ext_backfill_done_at` (миграция 0020).

## Данные

`panels`, `panel_communities` (выборка, страта, размер), `panel_posts` (текст, хеш нормализованного
текста, вложения со ссылками на фото/обложки/первые кадры, оригинал/репост с источником),
`panel_post_snapshots` (временные ряды счётчиков), `panel_media` (очередь медиа для pHash; скачивание —
только через шлагбаум CDN, отдельной командой после PR #19), `panel_community_snapshots`.

## Квоты

Каждый вызов учитывается в `vk_token_daily_usage` по паре токен + метод, включая вызовы внутри
`execute`. При `VK_METHOD_DAILY_LIMITS` метод на токене закрывается до полуночи (Москва) при доле
`VK_DAILY_BUDGET_FRACTION` (0,8) от лимита; пул переходит на другой токен.

## Значения на проде (решение Андрея 08.11)

```
VK_METHOD_DAILY_LIMITS=wall.get=20000,wall.getById=25000,execute=25000,groups.getById=25000,groups.search=1000
VK_DAILY_BUDGET_FRACTION=0.8
VK_STOP_TOKEN_ON_QUOTA_ERROR=true
```

`wall.get` — стоп на 16 000 вызовов в сутки на токен (80 % от 20 000); остальные — по наблюдениям,
официальных чисел в доступной документации нет. Первая ошибка 29 останавливает токен целиком до
полуночи (МСК): запись `vk_token_stopped_until_next_day` в логе и в `panel status`
(`tokens_stopped_until_next_day`, `vk_usage_today`).

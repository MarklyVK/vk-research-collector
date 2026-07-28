# Отчёт о pilot второго этапа

## Первый pilot: исходные лимиты 200/1000

Pilot с seed `20260728` выполнен 28.07.2026 на 35 уникальных approved-группах:
14 food_delivery, 15 customer_acquisition, 11 tender_support и все пять multi-label
групп. Сбор занял 2 минуты 18 секунд, включая реальное прерывание контейнера и resume
того же run `4c539596-288a-4141-a08a-f3e6887ad1b0` с сохранённых checkpoints.

| Сущность | Количество |
|---|---:|
| обновлённые группы | 35 |
| посты | 3 969 |
| вложения | 7 261 |
| memberships | 12 316 |
| уникальные пользователи | 12 285 |
| subscriptions | 0 (выключены до отдельного gate) |
| completed jobs | 12 385 |
| skipped jobs | 5 |
| failed jobs / retry | 0 / 0 |

Выполнен 131 VK API request. Пять `groups.getMembers` завершены конечным skip с
официальной ошибкой VK 15 `Access denied: group hide members`; scraping не применялся.
Посты, memberships и subscriptions не имеют дублей; rejected jobs и зависшие locks — 0.

## Первый capacity gate

Точный baseline получен восстановлением predpilot dump во временную БД: 85 777 431
байт. После pilot: 117 513 239 байт; прирост 31 735 808 байт. Линейная стратифицированная
экстраполяция на 12 260 групп с коэффициентом 1,30 для индексов/WAL/миграций даёт
14 537 357 656 байт (13,54 GiB). Это выше безопасного лимита 7 GiB.

Capacity gate: **failed**. Full run `301fe7a5-be50-4b31-9640-147e067c4045` только
спланирован и оставлен в `paused_capacity_limit`; worker дополнительно отвергает его без
`capacity_gate=passed`. Рекомендуемый следующий pilot: posts=100, members=200,
subscriptions=false. Снимать паузу до нового измерения запрещено.

Машиночитаемые результаты: `exports/stage2-pilot/pilot-summary.json` и
`exports/stage2-pilot/capacity-estimate.json`.

## Уменьшенный repilot: лимиты 100/200

Чтобы повторные upsert не занизили прирост, `backups/stage2-pilot-20260728-002754Z.dump`
восстановлен в отдельную БД `vk_research_repilot_0836`. После migration 0004 baseline
содержал 12 260 approved-групп и ноль posts/members/users stage 2.

Repilot `b09b119a-3e5b-408e-a6ad-a327888c57fd` завершён за 70,5 секунды:

| Показатель | Значение |
|---|---:|
| выбранные группы | 35 |
| completed / skipped / failed jobs | 4 413 / 5 / 0 |
| VK requests / retries | 105 / 0 |
| посты / attachments | 2 367 / 4 432 |
| memberships / users | 4 319 / 4 313 |
| subscriptions | 0 (выключены) |
| БД до / после | 85 228 567 / 94 206 999 байт |
| прирост | 8 978 432 байта |

Пять skips — ожидаемый `groups.getMembers` error 15. Дубли posts, memberships и
subscriptions, rejected jobs и зависшие locks — 0. Прогноз с резервом 30% равен
4 173 749 973 байтам (3,89 GiB), что ниже safe limit 7 GiB: capacity gate `passed`.

Разрешены scopes groups, posts (до 100), members (до 200) и users. Subscriptions
остаются выключенными до отдельного измерения. Машиночитаемые результаты находятся в
`exports/stage2-repilot/`.

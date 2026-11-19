# ADR-0005: Схема ClickHouse: сырые события + агрегаты на materialized view

**Статус:** принято

## Решение
**`events`** — сырые события:
- `ReplacingMergeTree(received_at)` — дедупликация повторов (ADR-0002);
- `PARTITION BY toYYYYMM(timestamp)` — дешёвое удаление старых данных целыми партициями;
- `ORDER BY (project_id, name, toDate(timestamp), user_id, event_id)` — запросы всегда фильтруют по
  проекту и почти всегда по событию и периоду, поэтому читается узкий диапазон гранул;
- `TTL timestamp + INTERVAL 1 YEAR` — retention (агрегаты хранятся 2 года);
- bloom-filter индекс по `user_id` — поиск событий конкретного пользователя;
- `LowCardinality(String)` для имён событий, `Map(LowCardinality(String), String)` с ZSTD для свойств.

**`events_by_minute`** — `AggregatingMergeTree` + materialized view: `uniqState(event_id)` и
`uniqState(user_id)` по (проект, событие, минута). Число событий считается как число уникальных
`event_id`: MV видит каждую вставку, и `countState()` посчитал бы повторно отправленное клиентом
событие дважды, а merge сырой таблицы агрегаты уже не исправит. Плата — погрешность `uniq` на
больших объёмах (доли процента). Таймсерии и топ событий читают агрегаты —
на порядки меньше строк, чем сырые данные; `uniqMerge` корректно объединяет уникальных
пользователей между минутами/часами/днями.

**Воронки** — `windowFunnel(window)(timestamp, name = step1, ...)` по сырым событиям за один проход,
**retention** — агрегатная функция `retention(...)` по когорте.

Миграции — простые forward-only SQL-файлы с таблицей `schema_migrations`, выполняются Job'ом
`pulse migrate` до раскатки приложений.

# ADR-0006: Деплой в Kubernetes

**Статус:** принято

- **Kustomize** `base` + `overlays/dev` (kind, инфраструктура внутри кластера) + `overlays/prod`
  (managed Kafka/ClickHouse/Postgres, образ из GHCR) + `components/keda` (подключаемый компонент).
- **Миграции** — отдельный `Job` (Argo CD Sync hook в волне `-1`: после ConfigMap/Secret, до приложений), а не init-container в каждом pod'е:
  нет гонок миграций при раскатке N реплик.
- **Автомасштабирование:** API — HPA по CPU; `writer` — **KEDA по consumer lag** Kafka (lag — это и
  есть «свежесть данных» для пользователя), максимум реплик = числу партиций.
- **Probes:** `startupProbe` (медленный старт — вычитка реестра ключей), `readinessProbe` → `/readyz`
  (реестр ключей догнал топик / зависимости подключены), `livenessProbe` → `/healthz`.
  У воркеров (без HTTP API) — встроенный мини-HTTP-сервер для probe'ов и `/metrics`.
- **Graceful shutdown:** `preStop: sleep 5` у API (дать kube-proxy убрать endpoint),
  SIGTERM → writer дописывает буфер и коммитит offset'ы.
- **PDB**, `maxUnavailable: 0` при rolling update, `topologySpreadConstraints` по нодам.
- **Безопасность:** non-root UID 10001, `readOnlyRootFilesystem`, drop ALL capabilities, seccomp
  `RuntimeDefault`, без токена ServiceAccount, NetworkPolicy для воркеров, секреты — отдельным
  `Secret` (в prod — External Secrets).

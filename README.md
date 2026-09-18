# Arvan SMS API

مونولیت مازولار Go با لایه‌های `handler → usecase → repo` و Echo.

## ساختار

```
cmd/api/
internal/
  user/
  messaging/          # send, outbox worker, kafka consumer
utils/
  config/ database/ kafka/ redisx/ errs/ response/ metrics/
migrations/
```

## معماری پیامک (خلاصه)

```
HTTP Send
  → idempotency (Redis cache / Postgres truth + payload hash)
  → TX: lock balance → plan → debit → request + messages(queued) + outbox(pending)
  → پاسخ API

OutboxWorker
  → claim (SKIP LOCKED) → Kafka(sms.express|sms.normal) → published
  → بعد از N شکست → failed + sms.dlq

SMSConsumer (دو lane جدا)
  → claim message(sending) → provider (idempotent key=messageId, retry×3)
  → sent | failed(+DLQ) → commit offset
```

## Kafka

| Topic | نقش | Partition key |
|-------|-----|---------------|
| `sms.express` | ارسال فوری | `recipient` (ترتیب per-گیرنده) |
| `sms.normal` | ارسال عادی | `recipient` |
| `sms.dlq` | dead letter | messageId / aggregateId |

- پیش‌فرض broker: `KAFKA_NUM_PARTITIONS=6` در docker-compose
- تعداد worker هر lane با `min(config, partition_count)` سقف می‌خورد
- express و normal **consumer group جدا** دارند (بدون head-of-line blocking)
- balancer: Hash روی Key

## محدودیت‌ها (مهم)

### Provider idempotency
سیستم at-least-once است (outbox و Kafka ممکن است یک پیام را چند بار تحویل دهند).  
برای جلوگیری از SMS دوبل، `SMSSender.Send` کلید `idempotencyKey = msg:{messageId}` می‌گیرد.

- `LogSender` فعلی فقط داخل **همان process** dedupe می‌کند (بعد از restart از بین می‌رود).
- Provider واقعی **باید** idempotency سمت gateway پشتیبانی کند؛ در غیر این صورت با crash بین «ارسال موفق» و `MarkMessageSent`، پس از stale reclaim ممکن است SMS دوباره برود.

### Graceful shutdown
با SIGTERM/SIGINT: اول outbox/consumer با timeout ۱۰s تمام می‌شوند، بعد HTTP shutdown (۵s).  
`/health` در این بازه ممکن است هنوز 200 بدهد (readiness جدا پیاده نشده).

## Observability (OpenTelemetry)

استک محلی: **Grafana otel-lgtm** (Tempo + Prometheus + Grafana)

| سرویس | آدرس |
|--------|------|
| Grafana UI | http://localhost:3000 (admin / admin) |
| OTLP HTTP | `localhost:4318` |
| OTLP gRPC | `localhost:4317` |
| JSON counters داخل اپ | `GET http://localhost:8080/metrics` |

### چه چیزی instrument شده؟
- **Traces**: همهٔ HTTPها + `messaging.Send` + `outbox.ProcessOnce` + `sms.Process`
- **Metrics (OTLP)**: `messaging.sms_sent`, `messaging.outbox_published`, `messaging.send_accepted`, …

### Env
```bash
OTEL_ENABLED=true
OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4318   # داخل compose: otel-lgtm:4318
OTEL_INSECURE=true
OTEL_SERVICE_NAME=arvan-api
```

در Grafana: Explore → Tempo برای trace، Prometheus/Metrics برای متریک‌ها. فیلتر `service.name = arvan-api`.

## Retry / DLQ

| لایه | سیاست |
|------|--------|
| Provider | حداکثر ۳ تلاش با backoff؛ بعد `failed` + DLQ |
| Outbox | حداکثر `OUTBOX_MAX_ATTEMPTS` (پیش‌فرض ۱۰)؛ بعد `failed` + DLQ |
| Consumer `in_flight` | بدون commit + backoff (retry) |
| Payload خراب | commit + DLQ (بدون retry بی‌فایده) |

## Correctness قبل از stress

قبل از k6 این سوئیت را اجرا کن (نیاز به Postgres روی `5433`):

```bash
docker compose up -d postgres
go test ./internal/messaging/ -run 'TestCorrectness_' -count=1 -timeout 5m -v
```

پوشش: E2E شمارش دقیق، balance/concurrency، idempotency hammer، express/normal، قطع Kafka، fail بودن provider، تحویل تکراری.

> **Ingress throughput ≠ SMS delivery throughput.** اول correctness، بعد capacity.

## پاسخ HTTP

```json
{ "data": { } }
```

```json
{
  "error": {
    "code": "validation_error",
    "message": "validation failed",
    "fields": [{ "field": "name", "message": "required" }]
  }
}
```

## Docs

- Swagger UI: http://localhost:8080/swagger/
- OpenAPI: http://localhost:8080/openapi.yaml
- Postman: [`docs/postman/Arvan.postman_collection.json`](docs/postman/Arvan.postman_collection.json)

## Docker

```bash
docker compose down
docker compose up --build -d
```

- API: http://localhost:8080
- Postgres: `localhost:5433` (`postgres` / `postgres` / `arvan`)
- Redis: `localhost:6380`
- Kafka: `localhost:9094` (داخل شبکه compose: `kafka:9092`)

Migrationهای `001`–`003` روی init volume اعمال می‌شوند. برای volume قدیمی:

```bash
psql postgres://postgres:postgres@localhost:5433/arvan < migrations/003_outbox_claim_drop_reservations.sql
```

## اجرا بدون Docker

```bash
psql arvan < migrations/001_init.sql
psql arvan < migrations/002_seed_user.sql
psql arvan < migrations/003_outbox_claim_drop_reservations.sql

export ADDR=':8080'
export KAFKA_BROKERS=localhost:9094
go run ./cmd/api
```

## API

### Users
- `GET /users/:id`
- `POST /users/:id/add-balance` `{ "amount": 5000 }`

### Messages
- `POST /messages/send/otp`
- `POST /messages/send/text`
- `GET /messages` — فیلتر: `userId`, `requestId`, `status`, `type`, `deliveryMode`, `recipient`, `errorCode`

### Ops
- `GET /health`
- `GET /metrics`

### هزینه
- هر پیامک: **۱ تومان**
- موجودی ناکافی → پذیرش جزئی (`skipped`)
- Idempotency: Postgres source of truth، Redis فقط cache (+ تطبیق `payload_hash`)
- کسر موجودی اتمیک در Postgres (`SELECT … FOR UPDATE`)

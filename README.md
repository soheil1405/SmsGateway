# Arvan SMS API

مونولیت مازولار با لایه‌های `handler → usecase → repo` و Echo.

## ساختار

```
cmd/api/
internal/
  user/
    domain/
    handler.go usecase.go repo.go dto.go register.go
  messaging/
    domain/
    handler.go usecase.go repo.go dto.go plan.go register.go
utils/
  config/ database/ errs/ response/
migrations/
```

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

- Swagger UI (داخل خود API): http://localhost:8080/swagger/
- OpenAPI raw: http://localhost:8080/openapi.yaml
- فایل مشخصات: [`docs/swagger.yaml`](docs/swagger.yaml)
- Postman: [`docs/postman/Arvan.postman_collection.json`](docs/postman/Arvan.postman_collection.json)

## Docker

از ایمیج لوکال `postgres:16-alpine` استفاده می‌شود.

```bash
# اگر استک قدیمی arvan بالا است، اول پایین بیاور:
docker compose down

docker compose up --build -d
```

- API: http://localhost:8080
- Postgres روی میزبان: `localhost:5433` (یوزر/پسورد/دیتابیس: `postgres` / `postgres` / `arvan`)
- Seed یوزر هنگام init دیتابیس اعمال می‌شود

## اجرا بدون Docker

```bash
createdb arvan
psql arvan < migrations/001_init.sql
psql arvan < migrations/002_seed_user.sql

export ADDR=':8080'
# یا DATABASE_URL یا فیلدهای DB_*
go run ./cmd/api
```

## API

### Users
- `GET /users/:id`
- `POST /users/:id/add-balance` `{ "amount": 5000 }`

### Messages
- `POST /messages/send`
- `GET /messages` — فیلتر اختیاری:
  - `userId`, `requestId`, `status`, `type`, `deliveryMode`, `recipient`, `errorCode`

### هزینه
- OTP: 50
- Text normal: 20
- Text express: 30

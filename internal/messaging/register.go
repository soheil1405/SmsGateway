package messaging

import (
	"database/sql"

	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"

	"github.com/soheil/arvan/utils/kafka"
)

// Register ماژول messaging را به سرور Echo وصل می‌کند
// (ساخت repo، usecase، redis store و handler).
func Register(e *echo.Echo, db *sql.DB, rdb *redis.Client, producer *kafka.Producer) {
	repo := NewRepo(db)
	uc := NewUseCase(repo, NewRedisStore(rdb), producer)
	NewHandler(uc).Register(e)
}

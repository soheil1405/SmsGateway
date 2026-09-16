package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// مدت نگه‌داری نتیجهٔ idempotency در Redis
const idempotencyTTL = 24 * time.Hour

// RedisStore فقط کش idempotency را نگه می‌دارد (موجودی در Postgres است).
type RedisStore struct {
	client *redis.Client
}

// NewRedisStore یک store روی کلاینت Redis می‌سازد.
func NewRedisStore(client *redis.Client) *RedisStore {
	return &RedisStore{client: client}
}

func idempotencyKey(userID int64, key string) string {
	return fmt.Sprintf("idempotency:%d:%s", userID, key)
}

// GetIdempotentResult اگر نتیجهٔ قبلی وجود داشته باشد آن را برمی‌گرداند.
func (s *RedisStore) GetIdempotentResult(ctx context.Context, userID int64, key string) (*SendResult, bool, error) {
	raw, err := s.client.Get(ctx, idempotencyKey(userID, key)).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	var result SendResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, false, err
	}
	return &result, true, nil
}

// SaveIdempotentResult نتیجهٔ ارسال را برای درخواست‌های تکراری ذخیره می‌کند.
func (s *RedisStore) SaveIdempotentResult(ctx context.Context, userID int64, key string, result *SendResult) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return s.client.Set(ctx, idempotencyKey(userID, key), raw, idempotencyTTL).Err()
}

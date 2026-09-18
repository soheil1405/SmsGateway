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

type idempotentCache struct {
	Hash   string      `json:"hash"`
	Result *SendResult `json:"result"`
}

// GetIdempotentResult اگر نتیجهٔ قبلی با همان payload hash وجود داشته باشد برمی‌گرداند.
// hash متفاوت با همان کلید → miss تا لایهٔ DB تعارض را تشخیص دهد.
func (s *RedisStore) GetIdempotentResult(ctx context.Context, userID int64, key, hash string) (*SendResult, bool, error) {
	raw, err := s.client.Get(ctx, idempotencyKey(userID, key)).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	var cached idempotentCache
	if err := json.Unmarshal(raw, &cached); err != nil {
		// سازگاری با کش قدیمی که فقط SendResult بود
		var legacy SendResult
		if err2 := json.Unmarshal(raw, &legacy); err2 != nil {
			return nil, false, err
		}
		return &legacy, true, nil
	}
	if cached.Result == nil {
		return nil, false, nil
	}
	if hash != "" && cached.Hash != "" && cached.Hash != hash {
		return nil, false, nil
	}
	return cached.Result, true, nil
}

// SaveIdempotentResult نتیجهٔ ارسال را همراه hash برای درخواست‌های تکراری ذخیره می‌کند.
func (s *RedisStore) SaveIdempotentResult(ctx context.Context, userID int64, key, hash string, result *SendResult) error {
	raw, err := json.Marshal(idempotentCache{Hash: hash, Result: result})
	if err != nil {
		return err
	}
	return s.client.Set(ctx, idempotencyKey(userID, key), raw, idempotencyTTL).Err()
}

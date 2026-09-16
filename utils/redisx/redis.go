package redisx

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/soheil/arvan/utils/config"
)

// Connect کلاینت Redis می‌سازد و با Ping اتصال را تأیید می‌کند.
func Connect(cfg config.Redis) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{Addr: cfg.Addr})
	if err := client.Ping(context.Background()).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return client, nil
}

package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

// Config تنظیمات کلی برنامه را نگه می‌دارد.
type Config struct {
	Addr     string   // آدرس listen سرور HTTP
	Database Database
	Redis    Redis
	Kafka    Kafka
}

// Database تنظیمات اتصال Postgres است.
type Database struct {
	Host     string
	Port     string
	User     string
	Password string
	Name     string
	SSLMode  string
	URL      string // اگر پر باشد، به‌جای فیلدهای جدا استفاده می‌شود
}

// DSN رشتهٔ اتصال Postgres را می‌سازد.
func (d Database) DSN() string {
	if d.URL != "" {
		return d.URL
	}

	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(d.User, d.Password),
		Host:   fmt.Sprintf("%s:%s", d.Host, d.Port),
		Path:   "/" + d.Name,
	}
	q := u.Query()
	q.Set("sslmode", d.SSLMode)
	u.RawQuery = q.Encode()
	return u.String()
}

// Redis تنظیمات آدرس Redis است.
type Redis struct {
	Addr string
}

// Kafka تنظیمات بروکرها و نام تاپیک‌های ارسال پیامک است.
type Kafka struct {
	Brokers      []string
	TopicNormal  string
	TopicExpress string
}

// Load تنظیمات را از متغیرهای محیطی (با مقدار پیش‌فرض) می‌خواند.
func Load() Config {
	brokers := env("KAFKA_BROKERS", "localhost:9094")
	return Config{
		Addr: env("ADDR", ":8080"),
		Database: Database{
			URL:      os.Getenv("DATABASE_URL"),
			Host:     env("DB_HOST", "localhost"),
			Port:     env("DB_PORT", "5432"),
			User:     env("DB_USER", "postgres"),
			Password: env("DB_PASSWORD", "postgres"),
			Name:     env("DB_NAME", "arvan"),
			SSLMode:  env("DB_SSLMODE", "disable"),
		},
		Redis: Redis{
			Addr: env("REDIS_ADDR", "localhost:6380"),
		},
		Kafka: Kafka{
			Brokers:      splitCSV(brokers),
			TopicNormal:  env("KAFKA_TOPIC_NORMAL", "sms.normal"),
			TopicExpress: env("KAFKA_TOPIC_EXPRESS", "sms.express"),
		},
	}
}

// env مقدار متغیر محیطی را می‌خواند؛ در صورت خالی بودن، fallback برمی‌گرداند.
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// splitCSV یک رشتهٔ جدا‌شده با ویرگول را به اسلایس تمیز تبدیل می‌کند.
func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

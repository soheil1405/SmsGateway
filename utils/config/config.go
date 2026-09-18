package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// Config تنظیمات کلی برنامه را نگه می‌دارد.
type Config struct {
	Addr     string // آدرس listen سرور HTTP
	Database Database
	Redis    Redis
	Kafka    Kafka
	OTel     OTel
}

// OTel تنظیمات OpenTelemetry / OTLP است.
type OTel struct {
	Enabled  bool
	Endpoint string // host:port بدون scheme — مثلاً localhost:4318
	Insecure bool
	Service  string
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

// Kafka تنظیمات بروکرها، تاپیک‌ها و ظرفیت مصرف‌کنندهٔ هر کلاس ارسال است.
type Kafka struct {
	Brokers      []string
	TopicNormal  string
	TopicExpress string
	TopicDLQ     string

	// هر کلاس consumer group مستقل دارد تا express پشت صف normal نماند.
	GroupNormal  string
	GroupExpress string

	// ظرفیت نامتقارن: express بیشتر، ولی normal همیشه سهم حداقلی خودش را دارد.
	WorkersNormal  int
	WorkersExpress int

	// سقف تلاش outbox قبل از انتقال به DLQ.
	OutboxMaxAttempts int
}

// Load تنظیمات را از متغیرهای محیطی (با مقدار پیش‌فرض) می‌خواند.
func Load() Config {
	brokers := env("KAFKA_BROKERS", "localhost:9094")
	return Config{
		Addr: env("ADDR", ":8080"),
		Database: Database{
			URL:      os.Getenv("DATABASE_URL"),
			Host:     env("DB_HOST", "localhost"),
			Port:     env("DB_PORT", "5433"),
			User:     env("DB_USER", "postgres"),
			Password: env("DB_PASSWORD", "postgres"),
			Name:     env("DB_NAME", "arvan"),
			SSLMode:  env("DB_SSLMODE", "disable"),
		},
		Redis: Redis{
			Addr: env("REDIS_ADDR", "localhost:6380"),
		},
		Kafka: Kafka{
			Brokers:           splitCSV(brokers),
			TopicNormal:       env("KAFKA_TOPIC_NORMAL", "sms.normal"),
			TopicExpress:      env("KAFKA_TOPIC_EXPRESS", "sms.express"),
			TopicDLQ:          env("KAFKA_TOPIC_DLQ", "sms.dlq"),
			GroupNormal:       env("KAFKA_GROUP_NORMAL", "sms-normal-worker"),
			GroupExpress:      env("KAFKA_GROUP_EXPRESS", "sms-express-worker"),
			WorkersNormal:     envInt("SMS_WORKERS_NORMAL", 2),
			WorkersExpress:    envInt("SMS_WORKERS_EXPRESS", 6),
			OutboxMaxAttempts: envInt("OUTBOX_MAX_ATTEMPTS", 10),
		},
		OTel: OTel{
			Enabled:  envBool("OTEL_ENABLED", true),
			Endpoint: env("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4318"),
			Insecure: envBool("OTEL_INSECURE", true),
			Service:  env("OTEL_SERVICE_NAME", "arvan-api"),
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

// envInt مقدار عددی متغیر محیطی را می‌خواند؛ در صورت نامعتبر بودن، fallback.
func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

// envBool مقدار بولین متغیر محیطی را می‌خواند (1/true/yes/on).
func envBool(key string, fallback bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if v == "" {
		return fallback
	}
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
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

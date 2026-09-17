package domain

import (
	"encoding/json"
	"time"
)

// OutboxStatus وضعیت انتشار رویداد outbox است.
type OutboxStatus string

const (
	OutboxPending   OutboxStatus = "pending"   // منتظر انتشار
	OutboxPublished OutboxStatus = "published" // منتشر شده
	OutboxFailed    OutboxStatus = "failed"    // شکست در انتشار
)

// OutboxEvent رویداد قابل انتشار به صف پیام (الگوی transactional outbox) است.
type OutboxEvent struct {
	ID           int64
	AggregateID  int64
	Topic        string
	PartitionKey string
	Payload      json.RawMessage
	Status       OutboxStatus
	Attempts     int
	LastError    *string
	CreatedAt    time.Time
	PublishedAt  *time.Time
}

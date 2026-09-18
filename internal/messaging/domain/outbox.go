package domain

import (
	"encoding/json"
	"time"
)

// OutboxStatus وضعیت انتشار رویداد outbox است.
type OutboxStatus string

const (
	OutboxPending    OutboxStatus = "pending"    // منتظر انتشار
	OutboxPublishing OutboxStatus = "publishing" // در حال انتشار (claim شده)
	OutboxPublished  OutboxStatus = "published"  // منتشر شده
	OutboxFailed     OutboxStatus = "failed"     // شکست در انتشار
)

// OutboxEvent رویداد قابل انتشار به صف پیام (الگوی transactional outbox) است.
// EventType منطقی: sms.message (aggregate_id = message id).
type OutboxEvent struct {
	ID           int64
	AggregateID  int64 // شناسهٔ message
	Topic        string
	PartitionKey string
	Payload      json.RawMessage
	Status       OutboxStatus
	Attempts     int
	LastError    *string
	CreatedAt    time.Time
	PublishedAt  *time.Time
}

// OutboxEventTypeSMS نوع منطقی رویداد ارسال پیامک در outbox است.
const OutboxEventTypeSMS = "sms.message"

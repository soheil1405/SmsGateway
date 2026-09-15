package domain

import (
	"encoding/json"
	"time"
)

type OutboxStatus string

const (
	OutboxPending   OutboxStatus = "pending"
	OutboxPublished OutboxStatus = "published"
	OutboxFailed    OutboxStatus = "failed"
)

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

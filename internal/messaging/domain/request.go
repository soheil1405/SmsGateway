package domain

import (
	"encoding/json"
	"time"
)

type Request struct {
	ID             int64
	UserID         int64
	IdempotencyKey string
	PayloadHash    string
	TotalCost      int64
	AcceptedCount  int
	RejectedCount  int
	ResponseJSON   json.RawMessage
	CreatedAt      time.Time
}

package domain

import (
	"encoding/json"
	"time"
)

// Request یک درخواست ارسال دسته‌ای (با کلید idempotency) است.
type Request struct {
	ID             int64
	UserID         int64
	IdempotencyKey string
	PayloadHash    string
	TotalCost      int64
	AcceptedCount  int
	RejectedCount  int
	ResponseJSON   json.RawMessage // اسنپ‌شات پاسخ نهایی
	CreatedAt      time.Time
}

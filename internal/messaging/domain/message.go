package domain

import "time"

// MessageType نوع منطقی پیام (otp یا text) است.
type MessageType string

const (
	MessageTypeOTP  MessageType = "otp"
	MessageTypeText MessageType = "text"
)

// DeliveryMode نحوهٔ ارسال (فوری یا عادی) است.
type DeliveryMode string

const (
	DeliveryExpress DeliveryMode = "express"
	DeliveryNormal  DeliveryMode = "normal"
)

// MessageStatus وضعیت چرخهٔ عمر یک پیام است.
type MessageStatus string

const (
	MessageAccepted MessageStatus = "accepted" // پذیرفته در مرحلهٔ برنامه‌ریزی
	MessageQueued   MessageStatus = "queued"   // درج در DB و آمادهٔ انتشار
	MessageSending  MessageStatus = "sending"  // در حال ارسال توسط worker
	MessageSent     MessageStatus = "sent"     // ارسال موفق
	MessageFailed   MessageStatus = "failed"   // شکست در ارسال
	MessageRejected MessageStatus = "rejected" // رد (اعتبارسنجی یا موجودی)
)

// Message موجودیت اصلی یک پیامک در سیستم است.
type Message struct {
	ID           int64
	RequestID    int64
	UserID       int64
	Recipient    string
	Type         MessageType
	DeliveryMode DeliveryMode
	Text         string
	Status       MessageStatus
	Cost         int64
	ErrorCode    *string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// Valid بررسی می‌کند که وضعیت پیام یکی از مقادیر مجاز باشد.
func (s MessageStatus) Valid() bool {
	switch s {
	case MessageAccepted, MessageQueued, MessageSending, MessageSent, MessageFailed, MessageRejected:
		return true
	default:
		return false
	}
}

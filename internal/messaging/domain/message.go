package domain

import "time"

type MessageType string

const (
	MessageTypeOTP  MessageType = "otp"
	MessageTypeText MessageType = "text"
)

type DeliveryMode string

const (
	DeliveryExpress DeliveryMode = "express"
	DeliveryNormal  DeliveryMode = "normal"
)

type MessageStatus string

const (
	MessageAccepted MessageStatus = "accepted"
	MessageQueued   MessageStatus = "queued"
	MessageSending  MessageStatus = "sending"
	MessageSent     MessageStatus = "sent"
	MessageFailed   MessageStatus = "failed"
	MessageRejected MessageStatus = "rejected"
)

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

func (s MessageStatus) Valid() bool {
	switch s {
	case MessageAccepted, MessageQueued, MessageSending, MessageSent, MessageFailed, MessageRejected:
		return true
	default:
		return false
	}
}

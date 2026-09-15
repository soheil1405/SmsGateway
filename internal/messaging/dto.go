package messaging

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/soheil/arvan/utils/errs"
	"github.com/soheil/arvan/internal/messaging/domain"
)

// --- HTTP request ---

type sendMessagesRequest struct {
	UserID         string             `json:"userId"`
	IdempotencyKey string             `json:"idempotencyKey"`
	OTPMessages    []otpMessageGroup  `json:"otpMessages"`
	TextMessages   []textMessageGroup `json:"textMessages"`
}

type otpMessageGroup struct {
	Type       string         `json:"type"`
	Template   string         `json:"template"`
	Recipients []otpRecipient `json:"recipients"`
}

type otpRecipient struct {
	Mobile    string            `json:"mobile"`
	Variables map[string]string `json:"variables"`
}

type textMessageGroup struct {
	Type       string   `json:"type"`
	Text       string   `json:"text"`
	Recipients []string `json:"recipients"`
}

func (r sendMessagesRequest) validate() error {
	v := &errs.Validation{}

	if strings.TrimSpace(r.UserID) == "" {
		v.Add("userId", "required")
	} else if _, err := strconv.ParseInt(r.UserID, 10, 64); err != nil {
		v.Add("userId", "must be a valid integer")
	}

	if strings.TrimSpace(r.IdempotencyKey) == "" {
		v.Add("idempotencyKey", "required")
	}

	if len(r.OTPMessages) == 0 && len(r.TextMessages) == 0 {
		v.Add("messages", "at least one otpMessages or textMessages entry is required")
	}

	for i, g := range r.OTPMessages {
		prefix := fmt.Sprintf("otpMessages[%d]", i)
		if g.Type != "express" && g.Type != "normal" {
			v.Add(prefix+".type", "must be express or normal")
		}
		if strings.TrimSpace(g.Template) == "" {
			v.Add(prefix+".template", "required")
		}
		if len(g.Recipients) == 0 {
			v.Add(prefix+".recipients", "required")
		}
	}

	for i, g := range r.TextMessages {
		prefix := fmt.Sprintf("textMessages[%d]", i)
		if g.Type != "express" && g.Type != "normal" {
			v.Add(prefix+".type", "must be express or normal")
		}
		if strings.TrimSpace(g.Text) == "" {
			v.Add(prefix+".text", "required")
		}
		if len(g.Recipients) == 0 {
			v.Add(prefix+".recipients", "required")
		}
	}

	return v.Err()
}

func (r sendMessagesRequest) toCommand() SendCommand {
	userID, _ := strconv.ParseInt(r.UserID, 10, 64)

	otp := make([]OTPGroup, 0, len(r.OTPMessages))
	for _, g := range r.OTPMessages {
		recipients := make([]OTPRecipient, 0, len(g.Recipients))
		for _, rec := range g.Recipients {
			recipients = append(recipients, OTPRecipient{
				Mobile:    rec.Mobile,
				Variables: rec.Variables,
			})
		}
		otp = append(otp, OTPGroup{
			DeliveryMode: g.Type,
			Template:     g.Template,
			Recipients:   recipients,
		})
	}

	text := make([]TextGroup, 0, len(r.TextMessages))
	for _, g := range r.TextMessages {
		text = append(text, TextGroup{
			DeliveryMode: g.Type,
			Text:         g.Text,
			Recipients:   g.Recipients,
		})
	}

	return SendCommand{
		UserID:         userID,
		IdempotencyKey: r.IdempotencyKey,
		OTPMessages:    otp,
		TextMessages:   text,
	}
}

// --- HTTP response ---

type sendMessagesResponse struct {
	RequestID     int64                `json:"requestId"`
	TotalCost     int64                `json:"totalCost"`
	AcceptedCount int                  `json:"acceptedCount"`
	RejectedCount int                  `json:"rejectedCount"`
	Messages      []messageItemResponse `json:"messages"`
}

type messageItemResponse struct {
	ID           int64  `json:"id,omitempty"`
	Recipient    string `json:"recipient"`
	Type         string `json:"type"`
	DeliveryMode string `json:"deliveryMode"`
	Status       string `json:"status"`
	Cost         int64  `json:"cost"`
	ErrorCode    string `json:"errorCode,omitempty"`
	Text         string `json:"text,omitempty"`
}

type messageDetailResponse struct {
	ID           int64     `json:"id"`
	RequestID    int64     `json:"requestId"`
	UserID       int64     `json:"userId"`
	Recipient    string    `json:"recipient"`
	Type         string    `json:"type"`
	DeliveryMode string    `json:"deliveryMode"`
	Text         string    `json:"text"`
	Status       string    `json:"status"`
	Cost         int64     `json:"cost"`
	ErrorCode    string    `json:"errorCode,omitempty"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

func toSendResponse(r *SendResult) sendMessagesResponse {
	items := make([]messageItemResponse, 0, len(r.Messages))
	for _, m := range r.Messages {
		items = append(items, messageItemResponse{
			ID:           m.ID,
			Recipient:    m.Recipient,
			Type:         m.Type,
			DeliveryMode: m.DeliveryMode,
			Status:       m.Status,
			Cost:         m.Cost,
			ErrorCode:    m.ErrorCode,
			Text:         m.Text,
		})
	}
	return sendMessagesResponse{
		RequestID:     r.RequestID,
		TotalCost:     r.TotalCost,
		AcceptedCount: r.AcceptedCount,
		RejectedCount: r.RejectedCount,
		Messages:      items,
	}
}

func toMessageDetailResponse(m *domain.Message) messageDetailResponse {
	resp := messageDetailResponse{
		ID:           m.ID,
		RequestID:    m.RequestID,
		UserID:       m.UserID,
		Recipient:    m.Recipient,
		Type:         string(m.Type),
		DeliveryMode: string(m.DeliveryMode),
		Text:         m.Text,
		Status:       string(m.Status),
		Cost:         m.Cost,
		CreatedAt:    m.CreatedAt,
		UpdatedAt:    m.UpdatedAt,
	}
	if m.ErrorCode != nil {
		resp.ErrorCode = *m.ErrorCode
	}
	return resp
}

func toMessageListResponse(messages []domain.Message) []messageDetailResponse {
	out := make([]messageDetailResponse, 0, len(messages))
	for i := range messages {
		out = append(out, toMessageDetailResponse(&messages[i]))
	}
	return out
}

// --- list filter ---

type listMessagesRequest struct {
	UserID       *int64  `query:"userId"`
	RequestID    *int64  `query:"requestId"`
	Status       string  `query:"status"`
	Type         string  `query:"type"`
	DeliveryMode string  `query:"deliveryMode"`
	Recipient    string  `query:"recipient"`
	ErrorCode    string  `query:"errorCode"`
}

func (r listMessagesRequest) validate() error {
	v := &errs.Validation{}

	if r.Status != "" && !isMessageStatus(r.Status) {
		v.Add("status", "must be one of: accepted, queued, sending, sent, failed, rejected")
	}
	if r.Type != "" && r.Type != "otp" && r.Type != "text" {
		v.Add("type", "must be otp or text")
	}
	if r.DeliveryMode != "" && r.DeliveryMode != "express" && r.DeliveryMode != "normal" {
		v.Add("deliveryMode", "must be express or normal")
	}

	return v.Err()
}

func (r listMessagesRequest) toFilter() MessageFilter {
	return MessageFilter{
		UserID:       r.UserID,
		RequestID:    r.RequestID,
		Status:       r.Status,
		Type:         r.Type,
		DeliveryMode: r.DeliveryMode,
		Recipient:    r.Recipient,
		ErrorCode:    r.ErrorCode,
	}
}

func isMessageStatus(s string) bool {
	return domain.MessageStatus(s).Valid()
}

type MessageFilter struct {
	UserID       *int64
	RequestID    *int64
	Status       string
	Type         string
	DeliveryMode string
	Recipient    string
	ErrorCode    string
}

// --- UseCase command / result ---

type SendCommand struct {
	UserID         int64
	IdempotencyKey string
	OTPMessages    []OTPGroup
	TextMessages   []TextGroup
}

type OTPGroup struct {
	DeliveryMode string
	Template     string
	Recipients   []OTPRecipient
}

type OTPRecipient struct {
	Mobile    string
	Variables map[string]string
}

type TextGroup struct {
	DeliveryMode string
	Text         string
	Recipients   []string
}

type SendResult struct {
	RequestID     int64           `json:"requestId"`
	TotalCost     int64           `json:"totalCost"`
	AcceptedCount int             `json:"acceptedCount"`
	RejectedCount int             `json:"rejectedCount"`
	Messages      []MessageResult `json:"messages"`
}

type MessageResult struct {
	ID           int64  `json:"id,omitempty"`
	Recipient    string `json:"recipient"`
	Type         string `json:"type"`
	DeliveryMode string `json:"deliveryMode"`
	Status       string `json:"status"`
	Cost         int64  `json:"cost"`
	ErrorCode    string `json:"errorCode,omitempty"`
	Text         string `json:"text,omitempty"`
}

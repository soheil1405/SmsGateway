package messaging

import (
	"strconv"
	"strings"
	"time"

	"github.com/soheil/arvan/internal/messaging/domain"
	"github.com/soheil/arvan/utils/errs"
)

// --- بدنهٔ HTTP برای ارسال OTP ---

// sendOTPRequest بدنهٔ درخواست POST /messages/send/otp است.
type sendOTPRequest struct {
	UserID         string         `json:"userId"`
	IdempotencyKey string         `json:"idempotencyKey"`
	Type           string         `json:"type"` // express یا normal (در عمل OTP همیشه normal می‌شود)
	Template       string         `json:"template"`
	Recipients     []OTPRecipient `json:"recipients"`
}

// validate فیلدهای اجباری و مقادیر مجاز را بررسی می‌کند.
func (r sendOTPRequest) validate() error {
	v := &errs.Validation{}
	validateCommon(v, r.UserID, r.IdempotencyKey)

	if r.Type != "express" && r.Type != "normal" {
		v.Add("type", "must be express or normal")
	}
	if strings.TrimSpace(r.Template) == "" {
		v.Add("template", "required")
	}
	if len(r.Recipients) == 0 {
		v.Add("recipients", "required")
	}
	return v.Err()
}

// toCommand بدنهٔ HTTP را به فرمان داخلی UseCase تبدیل می‌کند.
func (r sendOTPRequest) toCommand() SendMsgRequest {
	userID, _ := strconv.ParseInt(r.UserID, 10, 64)
	return SendMsgRequest{
		UserID:         userID,
		IdempotencyKey: r.IdempotencyKey,
		OTP: &OTPPayload{
			DeliveryMode: r.Type,
			Template:     r.Template,
			Recipients:   r.Recipients, // بدون کپی اضافه
		},
	}
}

// --- بدنهٔ HTTP برای ارسال متن ---

// sendTextRequest بدنهٔ درخواست POST /messages/send/text است.
type sendTextRequest struct {
	UserID         string   `json:"userId"`
	IdempotencyKey string   `json:"idempotencyKey"`
	Type           string   `json:"type"` // express یا normal
	Text           string   `json:"text"`
	Recipients     []string `json:"recipients"`
}

func (r sendTextRequest) validate() error {
	v := &errs.Validation{}
	validateCommon(v, r.UserID, r.IdempotencyKey)

	if r.Type != "express" && r.Type != "normal" {
		v.Add("type", "must be express or normal")
	}
	if strings.TrimSpace(r.Text) == "" {
		v.Add("text", "required")
	}
	if len(r.Recipients) == 0 {
		v.Add("recipients", "required")
	}
	return v.Err()
}

func (r sendTextRequest) toCommand() SendMsgRequest {
	userID, _ := strconv.ParseInt(r.UserID, 10, 64)
	return SendMsgRequest{
		UserID:         userID,
		IdempotencyKey: r.IdempotencyKey,
		Text: &TextPayload{
			DeliveryMode: r.Type,
			Text:         r.Text,
			Recipients:   r.Recipients,
		},
	}
}

// validateCommon قوانین مشترک userId و idempotencyKey را اعمال می‌کند.
func validateCommon(v *errs.Validation, userID, idempotencyKey string) {
	if strings.TrimSpace(userID) == "" {
		v.Add("userId", "required")
	} else if _, err := strconv.ParseInt(userID, 10, 64); err != nil {
		v.Add("userId", "must be a valid integer")
	}
	if strings.TrimSpace(idempotencyKey) == "" {
		v.Add("idempotencyKey", "required")
	}
}

// --- پاسخ HTTP لیست پیام‌ها ---

// messageDetailResponse شکل کامل یک پیام در لیست است.
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

// toMessageDetailResponse دامنه را به پاسخ HTTP تبدیل می‌کند.
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

// toMessageListResponse لیست دامنه را به پاسخ HTTP تبدیل می‌کند.
func toMessageListResponse(messages []domain.Message) []messageDetailResponse {
	out := make([]messageDetailResponse, 0, len(messages))
	for i := range messages {
		out = append(out, toMessageDetailResponse(&messages[i]))
	}
	return out
}

// --- فیلتر لیست ---

// listMessagesRequest پارامترهای query برای GET /messages است.
type listMessagesRequest struct {
	UserID       *int64 `query:"userId"`
	RequestID    *int64 `query:"requestId"`
	Status       string `query:"status"`
	Type         string `query:"type"`
	DeliveryMode string `query:"deliveryMode"`
	Recipient    string `query:"recipient"`
	ErrorCode    string `query:"errorCode"`
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

// MessageFilter فیلتر داخلی برای کوئری لیست پیام‌ها است.
type MessageFilter struct {
	UserID       *int64
	RequestID    *int64
	Status       string
	Type         string
	DeliveryMode string
	Recipient    string
	ErrorCode    string
}

// --- فرمان و نتیجهٔ UseCase ---

// SendMsgRequest فرمان داخلی ارسال (از OTP یا Text) است.
type SendMsgRequest struct {
	UserID         int64
	IdempotencyKey string
	OTP            *OTPPayload
	Text           *TextPayload
}

// OTPPayload محتوای ارسال OTP است.
type OTPPayload struct {
	DeliveryMode string
	Template     string
	Recipients   []OTPRecipient
}

// OTPRecipient یک گیرنده OTP با متغیرهای قالب است.
type OTPRecipient struct {
	Mobile    string            `json:"mobile"`
	Variables map[string]string `json:"variables"`
}

// TextPayload محتوای ارسال پیام متنی است.
type TextPayload struct {
	DeliveryMode string
	Text         string
	Recipients   []string
}

// SendResult پاسخ استاندارد ارسال (هم HTTP و هم کش idempotency) است.
type SendResult struct {
	RequestID     int64           `json:"requestId"`
	TotalCost     int64           `json:"totalCost"`
	AcceptedCount int             `json:"acceptedCount"`
	RejectedCount int             `json:"rejectedCount"`
	SkippedCount  int             `json:"skippedCount"`
	Messages      []MessageResult `json:"messages"` // پذیرفته و صف‌شده
	Skipped       []MessageResult `json:"skipped"`  // رد / موجودی ناکافی
}

// MessageResult خلاصهٔ وضعیت یک گیرنده در پاسخ ارسال است.
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

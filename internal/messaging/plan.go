package messaging

import (
	"strings"

	"github.com/soheil/arvan/internal/messaging/domain"
)

// smsCost هزینهٔ یکنواخت هر پیامک (تومان).
const smsCost int64 = 1

// plannedSend نتیجهٔ برنامه‌ریزی ارسال قبل از persist و Kafka است.
type plannedSend struct {
	Payable       []domain.Message // پیام‌های قابل پذیرش (موجودی کافی)
	Skipped       []MessageResult  // ردشده/ردشده به‌خاطر موجودی — فقط برای پاسخ API
	TotalCost     int64            // تعداد پذیرفته‌شده × smsCost
	RejectedCount int              // تعداد invalid (مثلاً موبایل نامعتبر)
	SkippedCount  int              // تعداد insufficient_balance
}

// planSend در یک پاس پیام‌ها را می‌سازد و payable/skipped را جدا می‌کند.
// هزینه با ضرب (accepted × smsCost) حساب می‌شود، نه با جمع داخل لوپ.
func planSend(req SendMsgRequest, available int64) plannedSend {
	// حداکثر تعداد پیامی که با موجودی فعلی می‌توان پذیرفت
	affordable := available / smsCost
	if affordable < 0 {
		affordable = 0
	}

	n := recipientCount(req)
	// ظرفیت Payable + Skipped = تعداد کل گیرندگان درخواست
	payableCap := int(min64(affordable, int64(n)))
	out := plannedSend{
		Payable: make([]domain.Message, 0, payableCap),
		Skipped: make([]MessageResult, 0, n-payableCap),
	}

	var accepted int64

	if req.OTP != nil {
		for _, recipient := range req.OTP.Recipients {
			planOne(
				&out,
				&accepted,
				affordable,
				buildOTPCandidate(req.UserID, req.OTP.Template, recipient),
			)
		}
	}

	if req.Text != nil {
		mode := domain.DeliveryMode(req.Text.DeliveryMode)
		for _, mobile := range req.Text.Recipients {
			planOne(
				&out,
				&accepted,
				affordable,
				buildTextCandidate(req.UserID, mode, req.Text.Text, mobile),
			)
		}
	}

	out.TotalCost = accepted * smsCost
	return out
}

// planOne یک نامزد را در payable یا skipped قرار می‌دهد.
func planOne(out *plannedSend, accepted *int64, affordable int64, msg domain.Message) {
	// قبلاً به‌خاطر اعتبارسنجی رد شده (مثلاً موبایل نامعتبر)
	if msg.Status == domain.MessageRejected {
		out.RejectedCount++
		out.Skipped = append(out.Skipped, toMessageResult(&msg))
		return
	}

	// هنوز ظرفیت موجودی داریم
	if *accepted < affordable {
		*accepted++
		out.Payable = append(out.Payable, msg)
		return
	}

	// موجودی کافی نیست — در پاسخ می‌آید ولی به DB/Kafka نمی‌رود
	code := "insufficient_balance"
	msg.Status = domain.MessageRejected
	msg.ErrorCode = &code
	msg.Cost = 0
	out.SkippedCount++
	out.Skipped = append(out.Skipped, toMessageResult(&msg))
}

// recipientCount تعداد کل گیرندگان درخواست را برمی‌گرداند.
func recipientCount(req SendMsgRequest) int {
	n := 0
	if req.OTP != nil {
		n += len(req.OTP.Recipients)
	}
	if req.Text != nil {
		n += len(req.Text.Recipients)
	}
	return n
}

// buildOTPCandidate پیام OTP را به متن عادی (delivery=normal) تبدیل می‌کند.
func buildOTPCandidate(userID int64, template string, recipient OTPRecipient) domain.Message {
	if !isValidMobile(recipient.Mobile) {
		return rejectedMessage(userID, recipient.Mobile, domain.MessageTypeText, domain.DeliveryNormal, "", "invalid_mobile")
	}
	return domain.Message{
		UserID:       userID,
		Recipient:    recipient.Mobile,
		Type:         domain.MessageTypeText,
		DeliveryMode: domain.DeliveryNormal,
		Text:         renderTemplate(template, recipient.Variables),
		Status:       domain.MessageAccepted,
		Cost:         smsCost,
	}
}

// buildTextCandidate یک پیام متنی را برای گیرنده می‌سازد.
func buildTextCandidate(userID int64, mode domain.DeliveryMode, text, mobile string) domain.Message {
	if !isValidMobile(mobile) {
		return rejectedMessage(userID, mobile, domain.MessageTypeText, mode, text, "invalid_mobile")
	}
	return domain.Message{
		UserID:       userID,
		Recipient:    mobile,
		Type:         domain.MessageTypeText,
		DeliveryMode: mode,
		Text:         text,
		Status:       domain.MessageAccepted,
		Cost:         smsCost,
	}
}

// rejectedMessage پیام ردشده برای پاسخ API (بدون هزینه) می‌سازد.
func rejectedMessage(
	userID int64,
	recipient string,
	msgType domain.MessageType,
	mode domain.DeliveryMode,
	text, code string,
) domain.Message {
	errCode := code
	return domain.Message{
		UserID:       userID,
		Recipient:    recipient,
		Type:         msgType,
		DeliveryMode: mode,
		Text:         text,
		Status:       domain.MessageRejected,
		ErrorCode:    &errCode,
		Cost:         0,
	}
}

// renderTemplate جایگذاری متغیرها در قالب OTP را انجام می‌دهد.
// هم {{key}} و هم {key} پشتیبانی می‌شود.
func renderTemplate(tmpl string, vars map[string]string) string {
	out := tmpl
	for key, value := range vars {
		out = strings.ReplaceAll(out, "{{"+key+"}}", value)
		out = strings.ReplaceAll(out, "{"+key+"}", value)
	}
	return out
}

// isValidMobile موبایل را به‌صورت ساده (حداقل ۱۰ رقم عددی) اعتبارسنجی می‌کند.
func isValidMobile(mobile string) bool {
	mobile = strings.TrimSpace(mobile)
	if len(mobile) < 10 {
		return false
	}
	for _, c := range mobile {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// kafkaTopic تاپیک مناسب بر اساس حالت ارسال را برمی‌گرداند.
func kafkaTopic(mode domain.DeliveryMode, normal, express string) string {
	if mode == domain.DeliveryExpress {
		return express
	}
	return normal
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

package messaging

import (
	"strings"

	"github.com/soheil/arvan/internal/messaging/domain"
)

const (
	costOTP         int64 = 50
	costTextNormal  int64 = 20
	costTextExpress int64 = 30
)

func buildMessages(cmd SendCommand) []domain.Message {
	var out []domain.Message

	for _, group := range cmd.OTPMessages {
		mode := domain.DeliveryMode(group.DeliveryMode)
		for _, recipient := range group.Recipients {
			out = append(out, buildOTPMessage(cmd.UserID, mode, group.Template, recipient))
		}
	}

	for _, group := range cmd.TextMessages {
		mode := domain.DeliveryMode(group.DeliveryMode)
		for _, mobile := range group.Recipients {
			out = append(out, buildTextMessage(cmd.UserID, mode, group.Text, mobile))
		}
	}

	return out
}

func buildOTPMessage(userID int64, mode domain.DeliveryMode, template string, recipient OTPRecipient) domain.Message {
	if !isValidMobile(recipient.Mobile) {
		return rejectedMessage(userID, recipient.Mobile, domain.MessageTypeOTP, mode, "", "invalid_mobile")
	}

	return domain.Message{
		UserID:       userID,
		Recipient:    recipient.Mobile,
		Type:         domain.MessageTypeOTP,
		DeliveryMode: mode,
		Text:         renderTemplate(template, recipient.Variables),
		Status:       domain.MessageAccepted,
		Cost:         costOTP,
	}
}

func buildTextMessage(userID int64, mode domain.DeliveryMode, text, mobile string) domain.Message {
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
		Cost:         textCost(mode),
	}
}

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
	}
}

func textCost(mode domain.DeliveryMode) int64 {
	if mode == domain.DeliveryExpress {
		return costTextExpress
	}
	return costTextNormal
}

func renderTemplate(tmpl string, vars map[string]string) string {
	out := tmpl
	for key, value := range vars {
		out = strings.ReplaceAll(out, "{{"+key+"}}", value)
		out = strings.ReplaceAll(out, "{"+key+"}", value)
	}
	return out
}

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

func sumAcceptedCost(messages []domain.Message) int64 {
	var total int64
	for _, m := range messages {
		if m.Status == domain.MessageAccepted {
			total += m.Cost
		}
	}
	return total
}

func outboxTopic(mode domain.DeliveryMode) string {
	if mode == domain.DeliveryExpress {
		return "sms.express"
	}
	return "sms.normal"
}

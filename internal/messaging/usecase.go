package messaging

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/soheil/arvan/internal/messaging/domain"
	"github.com/soheil/arvan/utils/errs"
	"github.com/soheil/arvan/utils/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const idempotencyPollAttempts = 5
const idempotencyPollDelay = 20 * time.Millisecond

// UseCase منطق کسب‌وکار ارسال و لیست پیام‌ها را مدیریت می‌کند.
type UseCase struct {
	repo         *Repo
	redis        *RedisStore
	topicNormal  string
	topicExpress string
	metrics      *metrics.Counters
}

// NewUseCase یک نمونه UseCase با وابستگی‌های لازم می‌سازد.
func NewUseCase(repo *Repo, redis *RedisStore, topicNormal, topicExpress string, m *metrics.Counters) *UseCase {
	if m == nil {
		m = &metrics.Counters{}
	}
	return &UseCase{
		repo:         repo,
		redis:        redis,
		topicNormal:  topicNormal,
		topicExpress: topicExpress,
		metrics:      m,
	}
}

// Send جریان ارسال پیامک:
// ۱) کش Redis idempotency (با تطبیق payload hash)
// ۲) در صورت miss → PostgreSQL (source of truth)
// ۳) قفل موجودی → plan → debit → ذخیره request/messages/outbox
func (uc *UseCase) Send(ctx context.Context, req SendMsgRequest) (*SendResult, error) {
	ctx, span := otel.Tracer("arvan/messaging").Start(ctx, "messaging.Send",
		trace.WithAttributes(
			attribute.Int64("user.id", req.UserID),
			attribute.String("idempotency.key", req.IdempotencyKey),
		),
	)
	defer span.End()

	hash := payloadHash(req)

	if result, ok, err := uc.resolveIdempotent(ctx, req.UserID, req.IdempotencyKey, hash); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	} else if ok {
		uc.metrics.IncIdempotentHits(ctx, 1)
		span.SetAttributes(attribute.Bool("idempotent.hit", true))
		return result, nil
	}

	result, err := uc.persistSend(ctx, req, hash)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	uc.metrics.IncSendAccepted(ctx, 1)
	span.SetAttributes(
		attribute.Int64("request.id", result.RequestID),
		attribute.Int("accepted.count", result.AcceptedCount),
		attribute.Int64("total.cost", result.TotalCost),
	)
	_ = uc.redis.SaveIdempotentResult(ctx, req.UserID, req.IdempotencyKey, hash, result)
	return result, nil
}

func (uc *UseCase) resolveIdempotent(ctx context.Context, userID int64, key, hash string) (*SendResult, bool, error) {
	cached, ok, err := uc.redis.GetIdempotentResult(ctx, userID, key, hash)
	if err == nil && ok {
		return cached, true, nil
	}
	return uc.loadIdempotentFromDB(ctx, userID, key, hash)
}

func (uc *UseCase) loadIdempotentFromDB(ctx context.Context, userID int64, key, hash string) (*SendResult, bool, error) {
	req, err := uc.repo.FindRequestByIdempotency(ctx, userID, key)
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	// همان کلید با بدنهٔ متفاوت → تعارض
	if req.PayloadHash != "" && hash != "" && req.PayloadHash != hash {
		return nil, false, errs.ErrConflict
	}

	if len(req.ResponseJSON) == 0 {
		return nil, false, nil
	}

	var result SendResult
	if err := json.Unmarshal(req.ResponseJSON, &result); err != nil {
		return nil, false, err
	}

	_ = uc.redis.SaveIdempotentResult(ctx, userID, key, req.PayloadHash, &result)
	return &result, true, nil
}

// waitIdempotentFromDB بعد از unique conflict منتظر پر شدن response_json می‌ماند.
func (uc *UseCase) waitIdempotentFromDB(ctx context.Context, userID int64, key, hash string) (*SendResult, error) {
	for i := 0; i < idempotencyPollAttempts; i++ {
		result, ok, err := uc.loadIdempotentFromDB(ctx, userID, key, hash)
		if err != nil {
			return nil, err
		}
		if ok {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(idempotencyPollDelay):
		}
	}
	return nil, errs.ErrConflict
}

func (uc *UseCase) ListMessages(ctx context.Context, filter MessageFilter) ([]domain.Message, error) {
	return uc.repo.ListMessages(ctx, filter)
}

// persistSend موجودی را قفل/کسر می‌کند و request + messages + outbox را در یک TX ذخیره می‌کند.
func (uc *UseCase) persistSend(ctx context.Context, cmd SendMsgRequest, hash string) (*SendResult, error) {
	tx, err := uc.repo.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	balance, err := uc.repo.GetBalanceForUpdate(ctx, tx, cmd.UserID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, errs.ErrNotFound
		}
		return nil, err
	}

	planned := planSend(cmd, balance)
	if err := uc.repo.DebitBalance(ctx, tx, cmd.UserID, planned.TotalCost); err != nil {
		if errors.Is(err, ErrInsufficientBalance) {
			return nil, errs.ErrInsufficientBalance
		}
		return nil, err
	}

	req := &domain.Request{
		UserID:         cmd.UserID,
		IdempotencyKey: cmd.IdempotencyKey,
		PayloadHash:    hash,
		TotalCost:      planned.TotalCost,
	}
	if err := uc.repo.CreateRequest(ctx, tx, req); err != nil {
		if errors.Is(err, ErrConflict) {
			_ = tx.Rollback()
			return uc.waitIdempotentFromDB(ctx, cmd.UserID, cmd.IdempotencyKey, hash)
		}
		return nil, err
	}

	accepted := make([]MessageResult, 0, len(planned.Payable))
	for i := range planned.Payable {
		msg := &planned.Payable[i]
		msg.RequestID = req.ID
		msg.Status = domain.MessageQueued

		if err := uc.repo.CreateMessage(ctx, tx, msg); err != nil {
			return nil, err
		}
		accepted = append(accepted, toMessageResult(msg))

		payload, err := json.Marshal(map[string]any{
			"eventType":    domain.OutboxEventTypeSMS,
			"messageId":    msg.ID,
			"requestId":    msg.RequestID,
			"userId":       msg.UserID,
			"recipient":    msg.Recipient,
			"text":         msg.Text,
			"type":         msg.Type,
			"deliveryMode": msg.DeliveryMode,
			"cost":         msg.Cost,
		})
		if err != nil {
			return nil, err
		}

		outbox := &domain.OutboxEvent{
			AggregateID:  msg.ID,
			Topic:        kafkaTopic(msg.DeliveryMode, uc.topicNormal, uc.topicExpress),
			PartitionKey: msg.Recipient, // ترتیب per-گیرنده در پارتیشن
			Payload:      payload,
			Status:       domain.OutboxPending,
		}
		if err := uc.repo.CreateOutbox(ctx, tx, outbox); err != nil {
			return nil, err
		}
	}

	result := &SendResult{
		RequestID:     req.ID,
		TotalCost:     planned.TotalCost,
		AcceptedCount: len(accepted),
		RejectedCount: planned.RejectedCount,
		SkippedCount:  planned.SkippedCount,
		Messages:      accepted,
		Skipped:       planned.Skipped,
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	if err := uc.repo.UpdateRequestResponse(ctx, tx, req.ID, result.AcceptedCount, result.RejectedCount, planned.TotalCost, raw); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return result, nil
}

func toMessageResult(msg *domain.Message) MessageResult {
	item := MessageResult{
		ID:           msg.ID,
		Recipient:    msg.Recipient,
		Type:         string(msg.Type),
		DeliveryMode: string(msg.DeliveryMode),
		Status:       string(msg.Status),
		Cost:         msg.Cost,
		Text:         msg.Text,
	}
	if msg.ErrorCode != nil {
		item.ErrorCode = *msg.ErrorCode
	}
	return item
}

// payloadHash اثر انگشت بدنهٔ درخواست (بدون idempotency key) است.
func payloadHash(cmd SendMsgRequest) string {
	body := map[string]any{
		"userId": cmd.UserID,
	}
	if cmd.OTP != nil {
		body["otp"] = cmd.OTP
	}
	if cmd.Text != nil {
		body["text"] = cmd.Text
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return strconv.FormatInt(cmd.UserID, 10)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

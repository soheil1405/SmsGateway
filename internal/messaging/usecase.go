package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/soheil/arvan/internal/messaging/domain"
	"github.com/soheil/arvan/utils/errs"
	"github.com/soheil/arvan/utils/kafka"
)

// UseCase منطق کسب‌وکار ارسال و لیست پیام‌ها را مدیریت می‌کند.
type UseCase struct {
	repo  *Repo
	redis *RedisStore
	kafka *kafka.Producer
}

// NewUseCase یک نمونه UseCase با وابستگی‌های لازم می‌سازد.
func NewUseCase(repo *Repo, redis *RedisStore, producer *kafka.Producer) *UseCase {
	return &UseCase{
		repo:  repo,
		redis: redis,
		kafka: producer,
	}
}

// Send جریان ارسال پیامک:
// ۱) کش Redis idempotency
// ۲) در صورت miss → PostgreSQL (source of truth)
// ۳) در غیر این صورت: قفل موجودی → plan → debit → ذخیره → Kafka
func (uc *UseCase) Send(ctx context.Context, req SendMsgRequest) (*SendResult, error) {
	if result, ok, err := uc.resolveIdempotent(ctx, req.UserID, req.IdempotencyKey); err != nil {
		return nil, err
	} else if ok {
		return result, nil
	}

	result, err := uc.persistAndPublish(ctx, req)
	if err != nil {
		return nil, err
	}

	// بهترین تلاش برای پر کردن کش؛ شکست Redis پاسخ موفق را خراب نمی‌کند
	_ = uc.redis.SaveIdempotentResult(ctx, req.UserID, req.IdempotencyKey, result)
	return result, nil
}

// resolveIdempotent اول Redis (cache) و در صورت miss، PostgreSQL را چک می‌کند.
// ok=true یعنی نتیجهٔ قبلی پیدا شد.
func (uc *UseCase) resolveIdempotent(ctx context.Context, userID int64, key string) (*SendResult, bool, error) {
	cached, ok, err := uc.redis.GetIdempotentResult(ctx, userID, key)
	if err == nil && ok {
		return cached, true, nil
	}
	// خطای Redis مثل miss تلقی می‌شود؛ Postgres منبع حقیقت است

	return uc.loadIdempotentFromDB(ctx, userID, key)
}

// loadIdempotentFromDB نتیجه را از response_json می‌خواند و در Redis دوباره cache می‌کند.
func (uc *UseCase) loadIdempotentFromDB(ctx context.Context, userID int64, key string) (*SendResult, bool, error) {
	req, err := uc.repo.FindRequestByIdempotency(ctx, userID, key)
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if len(req.ResponseJSON) == 0 {
		return nil, false, nil
	}

	var result SendResult
	if err := json.Unmarshal(req.ResponseJSON, &result); err != nil {
		return nil, false, err
	}

	_ = uc.redis.SaveIdempotentResult(ctx, userID, key, &result)
	return &result, true, nil
}

// ListMessages پیام‌ها را با فیلتر اختیاری برمی‌گرداند.
func (uc *UseCase) ListMessages(ctx context.Context, filter MessageFilter) ([]domain.Message, error) {
	return uc.repo.ListMessages(ctx, filter)
}

// persistAndPublish موجودی را در Postgres قفل/کسر می‌کند، پیام‌های payable را ذخیره
// و بعد از commit به Kafka می‌فرستد. skipped فقط در پاسخ می‌آید.
func (uc *UseCase) persistAndPublish(ctx context.Context, cmd SendMsgRequest) (*SendResult, error) {
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
		TotalCost:      planned.TotalCost,
	}
	if err := uc.repo.CreateRequest(ctx, tx, req); err != nil {
		if errors.Is(err, ErrConflict) {
			// TX جاری rollback می‌شود (debit هم لغو)؛ نتیجهٔ برنده از Postgres خوانده می‌شود
			_ = tx.Rollback()
			result, ok, e := uc.loadIdempotentFromDB(ctx, cmd.UserID, cmd.IdempotencyKey)
			if e != nil {
				return nil, e
			}
			if ok {
				return result, nil
			}
			return nil, errs.ErrConflict
		}
		return nil, err
	}

	accepted := make([]MessageResult, 0, len(planned.Payable))
	kafkaBatch := make([]kafka.Message, 0, len(planned.Payable))
	for i := range planned.Payable {
		msg := &planned.Payable[i]
		msg.RequestID = req.ID
		msg.Status = domain.MessageQueued

		if err := uc.repo.CreateMessage(ctx, tx, msg); err != nil {
			return nil, err
		}
		accepted = append(accepted, toMessageResult(msg))

		payload, err := json.Marshal(map[string]any{
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
		kafkaBatch = append(kafkaBatch, kafka.Message{
			Topic: kafkaTopic(msg.DeliveryMode, uc.kafka.TopicNormal(), uc.kafka.TopicExpress()),
			Key:   strconv.FormatInt(msg.ID, 10),
			Value: payload,
		})
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

	if err := uc.kafka.PublishBatch(ctx, kafkaBatch); err != nil {
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

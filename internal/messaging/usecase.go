package messaging

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/soheil/arvan/internal/messaging/domain"
	"github.com/soheil/arvan/internal/user"
	"github.com/soheil/arvan/utils/errs"
)

type UseCase struct {
	repo     *Repo
	userRepo *user.Repo
}

func NewUseCase(repo *Repo, userRepo *user.Repo) *UseCase {
	return &UseCase{repo: repo, userRepo: userRepo}
}

func (uc *UseCase) Send(ctx context.Context, cmd SendCommand) (*SendResult, error) {
	payloadHash, err := hashCommand(cmd)
	if err != nil {
		return nil, err
	}

	if cached, ok, err := uc.tryIdempotent(ctx, cmd.UserID, cmd.IdempotencyKey, payloadHash); err != nil {
		return nil, err
	} else if ok {
		return cached, nil
	}

	if _, err := uc.userRepo.GetByID(ctx, cmd.UserID); err != nil {
		if errors.Is(err, user.ErrNotFound) {
			return nil, errs.ErrNotFound
		}
		return nil, err
	}

	messages := buildMessages(cmd)
	totalCost := sumAcceptedCost(messages)

	result, err := uc.persistSend(ctx, cmd, payloadHash, messages, totalCost)
	if errors.Is(err, ErrConflict) {
		// concurrent duplicate with same idempotency key
		cached, ok, loadErr := uc.tryIdempotent(ctx, cmd.UserID, cmd.IdempotencyKey, payloadHash)
		if loadErr != nil {
			return nil, loadErr
		}
		if ok {
			return cached, nil
		}
		return nil, errs.ErrConflict
	}
	return result, err
}

func (uc *UseCase) ListMessages(ctx context.Context, filter MessageFilter) ([]domain.Message, error) {
	return uc.repo.ListMessages(ctx, filter)
}

func (uc *UseCase) tryIdempotent(ctx context.Context, userID int64, key, hash string) (*SendResult, bool, error) {
	existing, err := uc.repo.FindRequestByIdempotency(ctx, userID, key)
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if existing.PayloadHash != hash {
		return nil, false, errs.ErrConflict
	}
	if len(existing.ResponseJSON) == 0 {
		return nil, false, errors.New("idempotent request exists but response is not ready")
	}

	var result SendResult
	if err := json.Unmarshal(existing.ResponseJSON, &result); err != nil {
		return nil, false, err
	}
	return &result, true, nil
}

func (uc *UseCase) persistSend(
	ctx context.Context,
	cmd SendCommand,
	payloadHash string,
	messages []domain.Message,
	totalCost int64,
) (*SendResult, error) {
	tx, err := uc.repo.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	req := &domain.Request{
		UserID:         cmd.UserID,
		IdempotencyKey: cmd.IdempotencyKey,
		PayloadHash:    payloadHash,
	}
	if err := uc.repo.CreateRequest(ctx, tx, req); err != nil {
		return nil, err
	}

	if err := uc.reserveBalance(ctx, tx, cmd.UserID, req.ID, totalCost); err != nil {
		return nil, err
	}

	results, accepted, rejected, err := uc.saveMessages(ctx, tx, req.ID, messages)
	if err != nil {
		return nil, err
	}

	if totalCost > 0 {
		if err := uc.repo.CommitReservation(ctx, tx, req.ID); err != nil {
			return nil, err
		}
	}

	result := &SendResult{
		RequestID:     req.ID,
		TotalCost:     totalCost,
		AcceptedCount: accepted,
		RejectedCount: rejected,
		Messages:      results,
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	if err := uc.repo.UpdateRequestResponse(ctx, tx, req.ID, accepted, rejected, totalCost, raw); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func (uc *UseCase) reserveBalance(ctx context.Context, tx *sql.Tx, userID, requestID, totalCost int64) error {
	if totalCost <= 0 {
		return nil
	}

	if err := uc.repo.ReserveUserBalance(ctx, tx, userID, totalCost); err != nil {
		return mapRepoErr(err)
	}

	reservation := &domain.BalanceReservation{
		UserID:    userID,
		RequestID: requestID,
		Amount:    totalCost,
		Status:    domain.ReservationPending,
		ExpiresAt: DefaultReservationExpiry(),
	}
	return uc.repo.CreateReservation(ctx, tx, reservation)
}

func (uc *UseCase) saveMessages(
	ctx context.Context,
	tx *sql.Tx,
	requestID int64,
	messages []domain.Message,
) ([]MessageResult, int, int, error) {
	results := make([]MessageResult, 0, len(messages))
	accepted, rejected := 0, 0

	for i := range messages {
		msg := &messages[i]
		msg.RequestID = requestID

		if err := uc.repo.CreateMessage(ctx, tx, msg); err != nil {
			return nil, 0, 0, err
		}

		if msg.Status == domain.MessageAccepted {
			accepted++
			if err := uc.enqueueOutbox(ctx, tx, msg); err != nil {
				return nil, 0, 0, err
			}
		} else {
			rejected++
		}

		results = append(results, toMessageResult(msg))
	}

	return results, accepted, rejected, nil
}

func (uc *UseCase) enqueueOutbox(ctx context.Context, tx *sql.Tx, msg *domain.Message) error {
	payload, err := json.Marshal(map[string]any{
		"messageId": msg.ID,
		"userId":    msg.UserID,
		"recipient": msg.Recipient,
		"text":      msg.Text,
		"type":      msg.Type,
	})
	if err != nil {
		return err
	}

	event := &domain.OutboxEvent{
		AggregateID:  msg.ID,
		Topic:        outboxTopic(msg.DeliveryMode),
		PartitionKey: strconv.FormatInt(msg.ID, 10),
		Payload:      payload,
		Status:       domain.OutboxPending,
	}
	return uc.repo.CreateOutbox(ctx, tx, event)
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

func hashCommand(cmd SendCommand) (string, error) {
	raw, err := json.Marshal(cmd)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func mapRepoErr(err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return errs.ErrNotFound
	case errors.Is(err, ErrInsufficientBalance):
		return errs.ErrInsufficientBalance
	case errors.Is(err, ErrConflict):
		return errs.ErrConflict
	default:
		return err
	}
}

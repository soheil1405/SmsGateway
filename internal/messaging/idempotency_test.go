package messaging

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/soheil/arvan/internal/messaging/domain"
)

func newTestRedis(t *testing.T) *RedisStore {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisStore(client)
}

func deleteUserCascade(t *testing.T, db *sql.DB, userID int64) {
	t.Helper()
	_, _ = db.Exec(`
		DELETE FROM outbox_events
		WHERE aggregate_id IN (SELECT id FROM messages WHERE user_id = $1)
	`, userID)
	_, _ = db.Exec(`DELETE FROM messages WHERE user_id = $1`, userID)
	_, _ = db.Exec(`DELETE FROM requests WHERE user_id = $1`, userID)
	_, _ = db.Exec(`DELETE FROM users WHERE id = $1`, userID)
}

func loadIdempotentForTest(ctx context.Context, repo *Repo, redis *RedisStore, userID int64, key string) (*SendResult, bool, error) {
	req, err := repo.FindRequestByIdempotency(ctx, userID, key)
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
	_ = redis.SaveIdempotentResult(ctx, userID, key, req.PayloadHash, &result)
	return &result, true, nil
}

// persistIdempotentOnce هستهٔ idempotent persist را بدون Kafka شبیه‌سازی می‌کند
// (همان منطق usecase برای unique conflict → خواندن response_json).
func persistIdempotentOnce(ctx context.Context, repo *Repo, redis *RedisStore, cmd SendMsgRequest) (*SendResult, error) {
	hash := payloadHash(cmd)
	if cached, ok, err := redis.GetIdempotentResult(ctx, cmd.UserID, cmd.IdempotencyKey, hash); err == nil && ok {
		return cached, nil
	}
	if result, ok, err := loadIdempotentForTest(ctx, repo, redis, cmd.UserID, cmd.IdempotencyKey); err != nil {
		return nil, err
	} else if ok {
		return result, nil
	}

	tx, err := repo.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	balance, err := repo.GetBalanceForUpdate(ctx, tx, cmd.UserID)
	if err != nil {
		return nil, err
	}

	planned := planSend(cmd, balance)
	if err := repo.DebitBalance(ctx, tx, cmd.UserID, planned.TotalCost); err != nil {
		return nil, err
	}

	req := &domain.Request{
		UserID:         cmd.UserID,
		IdempotencyKey: cmd.IdempotencyKey,
		PayloadHash:    hash,
		TotalCost:      planned.TotalCost,
	}
	if err := repo.CreateRequest(ctx, tx, req); err != nil {
		if errors.Is(err, ErrConflict) {
			_ = tx.Rollback()
			result, ok, e := loadIdempotentForTest(ctx, repo, redis, cmd.UserID, cmd.IdempotencyKey)
			if e != nil {
				return nil, e
			}
			if ok {
				return result, nil
			}
			return nil, ErrConflict
		}
		return nil, err
	}

	accepted := make([]MessageResult, 0, len(planned.Payable))
	for i := range planned.Payable {
		msg := &planned.Payable[i]
		msg.RequestID = req.ID
		msg.Status = domain.MessageQueued
		if err := repo.CreateMessage(ctx, tx, msg); err != nil {
			return nil, err
		}
		accepted = append(accepted, toMessageResult(msg))
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
	if err := repo.UpdateRequestResponse(ctx, tx, req.ID, result.AcceptedCount, result.RejectedCount, planned.TotalCost, raw); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	_ = redis.SaveIdempotentResult(ctx, cmd.UserID, cmd.IdempotencyKey, hash, result)
	return result, nil
}

func TestIdempotency_RedisMiss_LoadsFromPostgres(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	redisStore := newTestRedis(t)
	ctx := context.Background()

	userID := insertTestUser(t, db, 100)
	t.Cleanup(func() { deleteUserCascade(t, db, userID) })

	want := &SendResult{
		RequestID:     0,
		TotalCost:     2,
		AcceptedCount: 2,
		RejectedCount: 0,
		SkippedCount:  0,
		Messages: []MessageResult{
			{ID: 1, Recipient: "09120000001", Type: "text", DeliveryMode: "normal", Status: "queued", Cost: 1},
			{ID: 2, Recipient: "09120000002", Type: "text", DeliveryMode: "normal", Status: "queued", Cost: 1},
		},
	}
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}

	var requestID int64
	err = db.QueryRow(`
		INSERT INTO requests (user_id, idempotency_key, payload_hash, total_cost, accepted_count, rejected_count, response_json)
		VALUES ($1, $2, '', $3, $4, $5, $6)
		RETURNING id
	`, userID, "abc", want.TotalCost, want.AcceptedCount, want.RejectedCount, raw).Scan(&requestID)
	if err != nil {
		t.Fatalf("seed request: %v", err)
	}
	want.RequestID = requestID
	raw, _ = json.Marshal(want)
	_, err = db.Exec(`UPDATE requests SET response_json = $2 WHERE id = $1`, requestID, raw)
	if err != nil {
		t.Fatalf("update response_json: %v", err)
	}

	uc := NewUseCase(repo, redisStore, "sms.normal", "sms.express", nil)
	// Send با redis miss باید از Postgres بخواند و به Kafka نرسد
	got, err := uc.Send(ctx, SendMsgRequest{
		UserID:         userID,
		IdempotencyKey: "abc",
		Text: &TextPayload{
			DeliveryMode: "normal",
			Text:         "hi",
			Recipients:   []string{"09120000001", "09120000002"},
		},
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got.RequestID != want.RequestID || got.TotalCost != want.TotalCost || got.AcceptedCount != want.AcceptedCount {
		t.Fatalf("got %+v, want requestId=%d totalCost=%d accepted=%d", got, want.RequestID, want.TotalCost, want.AcceptedCount)
	}

	// موجودی نباید تغییر کند (فقط خواندن idempotent)
	if bal := readBalance(t, db, userID); bal != 100 {
		t.Fatalf("balance=%d, want 100 (no debit on idempotent replay)", bal)
	}

	// باید در Redis دوباره cache شده باشد
	hash := payloadHash(SendMsgRequest{
		UserID: userID,
		Text: &TextPayload{
			DeliveryMode: "normal",
			Text:         "hi",
			Recipients:   []string{"09120000001", "09120000002"},
		},
	})
	cached, ok, err := redisStore.GetIdempotentResult(ctx, userID, "abc", hash)
	if err != nil || !ok {
		t.Fatalf("expected redis recache, ok=%v err=%v", ok, err)
	}
	if cached.RequestID != want.RequestID {
		t.Fatalf("cached requestId=%d, want %d", cached.RequestID, want.RequestID)
	}
}

func TestIdempotency_ConcurrentSameKey_SingleDebit(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	redisStore := newTestRedis(t)
	ctx := context.Background()

	const (
		initialBalance int64 = 50
		idemKey              = "abc"
	)

	userID := insertTestUser(t, db, initialBalance)
	t.Cleanup(func() { deleteUserCascade(t, db, userID) })

	cmd := SendMsgRequest{
		UserID:         userID,
		IdempotencyKey: idemKey,
		Text: &TextPayload{
			DeliveryMode: "normal",
			Text:         "concurrent",
			Recipients:   []string{"09123334445", "09123334446"},
		},
	}

	var (
		wg      sync.WaitGroup
		results = make([]*SendResult, 2)
		errsOut = make([]error, 2)
	)

	wg.Add(2)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			defer wg.Done()
			res, err := persistIdempotentOnce(ctx, repo, redisStore, cmd)
			results[i] = res
			errsOut[i] = err
		}()
	}
	wg.Wait()

	for i, err := range errsOut {
		if err != nil {
			t.Fatalf("goroutine %d error: %v", i, err)
		}
		if results[i] == nil {
			t.Fatalf("goroutine %d nil result", i)
		}
	}

	if results[0].RequestID != results[1].RequestID {
		t.Fatalf("different request ids: %d vs %d", results[0].RequestID, results[1].RequestID)
	}
	if results[0].TotalCost != results[1].TotalCost {
		t.Fatalf("different totalCost: %d vs %d", results[0].TotalCost, results[1].TotalCost)
	}

	var reqCount int
	if err := db.QueryRow(`
		SELECT COUNT(*) FROM requests WHERE user_id = $1 AND idempotency_key = $2
	`, userID, idemKey).Scan(&reqCount); err != nil {
		t.Fatal(err)
	}
	if reqCount != 1 {
		t.Fatalf("requests count=%d, want 1", reqCount)
	}

	final := readBalance(t, db, userID)
	if final < 0 {
		t.Fatalf("balance negative: %d", final)
	}
	wantBalance := initialBalance - results[0].TotalCost
	if final != wantBalance {
		t.Fatalf("balance=%d, want %d (debited once, totalCost=%d)", final, wantBalance, results[0].TotalCost)
	}
}

func TestIdempotency_ConflictReturnsStoredResponseNotRaw409(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	redisStore := newTestRedis(t)
	ctx := context.Background()

	userID := insertTestUser(t, db, 20)
	t.Cleanup(func() { deleteUserCascade(t, db, userID) })

	cmd := SendMsgRequest{
		UserID:         userID,
		IdempotencyKey: fmt.Sprintf("conflict-%d", time.Now().UnixNano()),
		Text: &TextPayload{
			DeliveryMode: "normal",
			Text:         "x",
			Recipients:   []string{"09120000099"},
		},
	}

	first, err := persistIdempotentOnce(ctx, repo, redisStore, cmd)
	if err != nil {
		t.Fatal(err)
	}

	// Redis خالی تا مسیر Postgres/conflict اجباری شود
	emptyRedis := newTestRedis(t)
	second, err := persistIdempotentOnce(ctx, repo, emptyRedis, cmd)
	if err != nil {
		t.Fatalf("expected stored response, got error: %v", err)
	}
	if second.RequestID != first.RequestID {
		t.Fatalf("requestId=%d, want %d", second.RequestID, first.RequestID)
	}
	if bal := readBalance(t, db, userID); bal != 20-first.TotalCost {
		t.Fatalf("balance=%d, debit must not repeat", bal)
	}
}

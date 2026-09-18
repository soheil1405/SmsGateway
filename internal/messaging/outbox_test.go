package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/soheil/arvan/internal/messaging/domain"
)

type fakePublisher struct {
	mu        sync.Mutex
	published []publishedMsg
	fail      bool
	failOnce  bool
	calls     int
}

type publishedMsg struct {
	Topic string
	Key   string
	Value []byte
}

func (f *fakePublisher) Publish(ctx context.Context, topic, key string, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.fail || (f.failOnce && f.calls == 1) {
		return errors.New("kafka unavailable")
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	f.published = append(f.published, publishedMsg{Topic: topic, Key: key, Value: cp})
	return nil
}

func (f *fakePublisher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.published)
}

func processUserPending(t *testing.T, ctx context.Context, repo *Repo, pub MessagePublisher, userID int64) {
	t.Helper()
	events, err := repo.listPendingOutboxByUser(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	for i := range events {
		e := &events[i]
		if err := pub.Publish(ctx, e.Topic, e.PartitionKey, e.Payload); err != nil {
			_ = repo.RecordOutboxFailure(ctx, e.ID, err.Error())
			continue
		}
		if err := repo.MarkOutboxPublished(ctx, e.ID); err != nil {
			t.Fatalf("mark published: %v", err)
		}
	}
}

func countUserOutbox(t *testing.T, userID int64, status domain.OutboxStatus) int {
	t.Helper()
	db := openTestDB(t)
	var n int
	err := db.QueryRow(`
		SELECT COUNT(*)
		FROM outbox_events o
		JOIN messages m ON m.id = o.aggregate_id
		WHERE m.user_id = $1 AND o.status = $2
	`, userID, status).Scan(&n)
	if err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return n
}

func countUserMessages(t *testing.T, userID int64) int {
	t.Helper()
	db := openTestDB(t)
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM messages WHERE user_id = $1`, userID).Scan(&n); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	return n
}

func countUserRequests(t *testing.T, userID int64) int {
	t.Helper()
	db := openTestDB(t)
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM requests WHERE user_id = $1`, userID).Scan(&n); err != nil {
		t.Fatalf("count requests: %v", err)
	}
	return n
}

func TestOutbox_SendPersistsRequestMessagesOutbox(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	redisStore := newTestRedis(t)
	uc := NewUseCase(repo, redisStore, "sms.normal", "sms.express", nil)
	ctx := context.Background()

	userID := insertTestUser(t, db, 10)
	t.Cleanup(func() { deleteUserCascade(t, db, userID) })

	result, err := uc.Send(ctx, SendMsgRequest{
		UserID:         userID,
		IdempotencyKey: fmt.Sprintf("outbox-ok-%d", time.Now().UnixNano()),
		Text: &TextPayload{
			DeliveryMode: "normal",
			Text:         "hello",
			Recipients:   []string{"09121111111", "09122222222"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.AcceptedCount != 2 {
		t.Fatalf("accepted=%d, want 2", result.AcceptedCount)
	}
	if countUserRequests(t, userID) != 1 {
		t.Fatal("want 1 request")
	}
	if countUserMessages(t, userID) != 2 {
		t.Fatal("want 2 messages")
	}
	if countUserOutbox(t, userID, domain.OutboxPending) != 2 {
		t.Fatal("want 2 pending outbox events")
	}
	if bal := readBalance(t, db, userID); bal != 8 {
		t.Fatalf("balance=%d, want 8", bal)
	}
}

func TestOutbox_RollbackDropsAll(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	ctx := context.Background()

	userID := insertTestUser(t, db, 5)
	t.Cleanup(func() { deleteUserCascade(t, db, userID) })

	tx, err := repo.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}

	bal, err := repo.GetBalanceForUpdate(ctx, tx, userID)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.DebitBalance(ctx, tx, userID, 1); err != nil {
		t.Fatal(err)
	}

	req := &domain.Request{UserID: userID, IdempotencyKey: "rollback-key", TotalCost: 1}
	if err := repo.CreateRequest(ctx, tx, req); err != nil {
		t.Fatal(err)
	}
	msg := &domain.Message{
		RequestID: req.ID, UserID: userID, Recipient: "09120000001",
		Type: domain.MessageTypeText, DeliveryMode: domain.DeliveryNormal,
		Text: "x", Status: domain.MessageQueued, Cost: 1,
	}
	if err := repo.CreateMessage(ctx, tx, msg); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{"messageId": msg.ID})
	if err := repo.CreateOutbox(ctx, tx, &domain.OutboxEvent{
		AggregateID: msg.ID, Topic: "sms.normal", PartitionKey: "1",
		Payload: payload, Status: domain.OutboxPending,
	}); err != nil {
		t.Fatal(err)
	}

	_ = bal
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if countUserRequests(t, userID) != 0 || countUserMessages(t, userID) != 0 {
		t.Fatal("rollback should remove request/messages")
	}
	if countUserOutbox(t, userID, domain.OutboxPending) != 0 {
		t.Fatal("rollback should remove outbox")
	}
	if readBalance(t, db, userID) != 5 {
		t.Fatal("balance should be restored after rollback")
	}
}

func TestOutbox_SendSucceedsWithoutKafka(t *testing.T) {
	// Send دیگر Kafka را صدا نمی‌زند؛ موفقیت فقط وابسته به DB است.
	db := openTestDB(t)
	repo := NewRepo(db)
	uc := NewUseCase(repo, newTestRedis(t), "sms.normal", "sms.express", nil)
	ctx := context.Background()

	userID := insertTestUser(t, db, 3)
	t.Cleanup(func() { deleteUserCascade(t, db, userID) })

	_, err := uc.Send(ctx, SendMsgRequest{
		UserID:         userID,
		IdempotencyKey: fmt.Sprintf("no-kafka-%d", time.Now().UnixNano()),
		Text: &TextPayload{
			DeliveryMode: "express",
			Text:         "hi",
			Recipients:   []string{"09123333333"},
		},
	})
	if err != nil {
		t.Fatalf("Send should succeed without Kafka: %v", err)
	}
	pending := countUserOutbox(t, userID, domain.OutboxPending)
	published := countUserOutbox(t, userID, domain.OutboxPublished)
	// اگر app زنده روی همان DB باشد ممکن است worker بلافاصله publish کند
	if pending+published != 1 {
		t.Fatalf("want 1 outbox row (pending or published), pending=%d published=%d", pending, published)
	}
}

func TestOutbox_WorkerPublishesAndMarks(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	uc := NewUseCase(repo, newTestRedis(t), "sms.normal", "sms.express", nil)
	ctx := context.Background()

	userID := insertTestUser(t, db, 4)
	t.Cleanup(func() { deleteUserCascade(t, db, userID) })

	_, err := uc.Send(ctx, SendMsgRequest{
		UserID:         userID,
		IdempotencyKey: fmt.Sprintf("worker-%d", time.Now().UnixNano()),
		Text: &TextPayload{
			DeliveryMode: "normal",
			Text:         "w",
			Recipients:   []string{"09124444444", "09125555555"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	pub := &fakePublisher{}
	processUserPending(t, ctx, repo, pub, userID)

	if pub.count() != 2 {
		t.Fatalf("published=%d, want 2", pub.count())
	}
	if countUserOutbox(t, userID, domain.OutboxPending) != 0 {
		t.Fatal("no pending left")
	}
	if countUserOutbox(t, userID, domain.OutboxPublished) != 2 {
		t.Fatal("want 2 published")
	}
}

func TestOutbox_WorkerRetriesWhenKafkaFails(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	uc := NewUseCase(repo, newTestRedis(t), "sms.normal", "sms.express", nil)
	ctx := context.Background()

	userID := insertTestUser(t, db, 2)
	t.Cleanup(func() { deleteUserCascade(t, db, userID) })

	_, err := uc.Send(ctx, SendMsgRequest{
		UserID:         userID,
		IdempotencyKey: fmt.Sprintf("retry-%d", time.Now().UnixNano()),
		Text: &TextPayload{
			DeliveryMode: "normal",
			Text:         "r",
			Recipients:   []string{"09126666666"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	pub := &fakePublisher{fail: true}
	processUserPending(t, ctx, repo, pub, userID)
	if countUserOutbox(t, userID, domain.OutboxPending) != 1 {
		t.Fatal("should stay pending after kafka failure")
	}

	pub.fail = false
	processUserPending(t, ctx, repo, pub, userID)
	if countUserOutbox(t, userID, domain.OutboxPublished) != 1 {
		t.Fatal("should publish on retry")
	}
}

func TestOutbox_CrashBeforeMarkAllowsRepublish(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	uc := NewUseCase(repo, newTestRedis(t), "sms.normal", "sms.express", nil)
	ctx := context.Background()

	userID := insertTestUser(t, db, 2)
	t.Cleanup(func() { deleteUserCascade(t, db, userID) })

	_, err := uc.Send(ctx, SendMsgRequest{
		UserID:         userID,
		IdempotencyKey: fmt.Sprintf("crash-%d", time.Now().UnixNano()),
		Text: &TextPayload{
			DeliveryMode: "normal",
			Text:         "c",
			Recipients:   []string{"09127777777"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	events, err := repo.listPendingOutboxByUser(ctx, userID)
	if err != nil || len(events) == 0 {
		t.Fatalf("pending events: %v len=%d", err, len(events))
	}

	// شبیه‌سازی: publish موفق شد ولی قبل از mark سرویس crash کرد
	pub := &fakePublisher{}
	e := events[0]
	if err := pub.Publish(ctx, e.Topic, e.PartitionKey, e.Payload); err != nil {
		t.Fatal(err)
	}
	if countUserOutbox(t, userID, domain.OutboxPending) != 1 {
		t.Fatal("still pending because mark was not called")
	}

	processUserPending(t, ctx, repo, pub, userID)
	// دوباره publish شد (at-least-once) و بعد mark
	if pub.count() < 2 {
		t.Fatalf("expected republish, published=%d", pub.count())
	}
	if countUserOutbox(t, userID, domain.OutboxPublished) != 1 {
		t.Fatal("want published after second successful process")
	}
}

func TestOutbox_MaxAttemptsGoesToDLQ(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	uc := NewUseCase(repo, newTestRedis(t), "sms.normal", "sms.express", nil)
	ctx := context.Background()

	// رویدادهای یتیم تست‌های قبلی را پاک کن تا claim سراسری قاطی نشود
	if _, err := db.Exec(`DELETE FROM outbox_events`); err != nil {
		t.Fatalf("cleanup outbox: %v", err)
	}

	userID := insertTestUser(t, db, 2)
	t.Cleanup(func() { deleteUserCascade(t, db, userID) })

	_, err := uc.Send(ctx, SendMsgRequest{
		UserID:         userID,
		IdempotencyKey: fmt.Sprintf("dlq-%d", time.Now().UnixNano()),
		Text: &TextPayload{
			DeliveryMode: "normal",
			Text:         "d",
			Recipients:   []string{"09128888888"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	pub := &fakePublisher{fail: true}
	dlq := &fakeDLQ{}
	worker := NewOutboxWorker(repo, pub, dlq, time.Millisecond, 10, 2, nil)

	// claim#1 fail → pending؛ claim#2 fail با attempts>=2 → DLQ
	for i := 0; i < 5; i++ {
		if err := worker.ProcessOnce(ctx); err != nil {
			t.Fatal(err)
		}
		if countUserOutbox(t, userID, domain.OutboxFailed) == 1 {
			break
		}
	}

	if countUserOutbox(t, userID, domain.OutboxFailed) != 1 {
		t.Fatalf("want 1 failed outbox, pending=%d published=%d failed=%d",
			countUserOutbox(t, userID, domain.OutboxPending),
			countUserOutbox(t, userID, domain.OutboxPublished),
			countUserOutbox(t, userID, domain.OutboxFailed),
		)
	}
	if dlq.len() < 1 {
		t.Fatal("expected DLQ publish")
	}
}

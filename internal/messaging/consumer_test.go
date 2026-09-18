package messaging

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/soheil/arvan/internal/messaging/domain"
	"github.com/soheil/arvan/utils/kafka"
)

// --- کمک‌کننده‌ها ---

type countingSender struct {
	calls atomic.Int64
	fail  bool
}

func (s *countingSender) Send(_ context.Context, _, _, _ string) error {
	s.calls.Add(1)
	if s.fail {
		return errors.New("provider down")
	}
	return nil
}

func seedQueuedMessage(t *testing.T, db *sql.DB, userID int64, mode domain.DeliveryMode) int64 {
	t.Helper()

	var requestID int64
	err := db.QueryRow(`
		INSERT INTO requests (user_id, idempotency_key, payload_hash, total_cost, accepted_count, rejected_count)
		VALUES ($1, $2, '', 1, 1, 0)
		RETURNING id
	`, userID, fmt.Sprintf("consumer-%d", time.Now().UnixNano())).Scan(&requestID)
	if err != nil {
		t.Fatalf("seed request: %v", err)
	}

	var messageID int64
	err = db.QueryRow(`
		INSERT INTO messages (request_id, user_id, recipient, type, delivery_mode, text, status, cost)
		VALUES ($1, $2, '09120000001', 'text', $3, 'hi', 'queued', 1)
		RETURNING id
	`, requestID, userID, mode).Scan(&messageID)
	if err != nil {
		t.Fatalf("seed message: %v", err)
	}
	return messageID
}

func messageStatus(t *testing.T, db *sql.DB, id int64) (string, string) {
	t.Helper()
	var status string
	var code sql.NullString
	if err := db.QueryRow(`SELECT status, error_code FROM messages WHERE id = $1`, id).Scan(&status, &code); err != nil {
		t.Fatalf("read message status: %v", err)
	}
	return status, code.String
}

func smsPayload(t *testing.T, messageID int64, mode string) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"eventType":    domain.OutboxEventTypeSMS,
		"messageId":    messageID,
		"recipient":    "09120000001",
		"text":         "hi",
		"type":         "text",
		"deliveryMode": mode,
		"cost":         1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// --- تست پردازش پیام ---

func TestConsumer_ProcessMarksSent(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	ctx := context.Background()

	userID := insertTestUser(t, db, 5)
	t.Cleanup(func() { deleteUserCascade(t, db, userID) })
	messageID := seedQueuedMessage(t, db, userID, domain.DeliveryNormal)

	sender := &countingSender{}
	processor := NewMessageProcessor(repo, sender, nil, nil)

	if err := processor.Process(ctx, smsPayload(t, messageID, "normal")); err != nil {
		t.Fatal(err)
	}

	status, _ := messageStatus(t, db, messageID)
	if status != string(domain.MessageSent) {
		t.Fatalf("status=%s, want sent", status)
	}
	if sender.calls.Load() != 1 {
		t.Fatalf("sender calls=%d, want 1", sender.calls.Load())
	}
}

func TestConsumer_DuplicateDeliverySendsOnce(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	ctx := context.Background()

	userID := insertTestUser(t, db, 5)
	t.Cleanup(func() { deleteUserCascade(t, db, userID) })
	messageID := seedQueuedMessage(t, db, userID, domain.DeliveryExpress)

	sender := &countingSender{}
	processor := NewMessageProcessor(repo, sender, nil, nil)
	payload := smsPayload(t, messageID, "express")

	for i := 0; i < 3; i++ {
		if err := processor.Process(ctx, payload); err != nil {
			t.Fatalf("process %d: %v", i, err)
		}
	}

	if sender.calls.Load() != 1 {
		t.Fatalf("sender calls=%d, want 1 (at-least-once delivery must be idempotent)", sender.calls.Load())
	}
	if status, _ := messageStatus(t, db, messageID); status != string(domain.MessageSent) {
		t.Fatalf("status=%s, want sent", status)
	}
}

func TestConsumer_ProviderFailureMarksFailed(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	ctx := context.Background()

	userID := insertTestUser(t, db, 5)
	t.Cleanup(func() { deleteUserCascade(t, db, userID) })
	messageID := seedQueuedMessage(t, db, userID, domain.DeliveryNormal)

	processor := NewMessageProcessor(repo, &countingSender{fail: true}, nil, nil)
	if err := processor.Process(ctx, smsPayload(t, messageID, "normal")); err != nil {
		t.Fatal(err)
	}

	status, code := messageStatus(t, db, messageID)
	if status != string(domain.MessageFailed) {
		t.Fatalf("status=%s, want failed", status)
	}
	if code != "provider_error" {
		t.Fatalf("errorCode=%q, want provider_error", code)
	}
}

func TestConsumer_StaleSendingIsReclaimed(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	ctx := context.Background()

	userID := insertTestUser(t, db, 5)
	t.Cleanup(func() { deleteUserCascade(t, db, userID) })
	messageID := seedQueuedMessage(t, db, userID, domain.DeliveryNormal)

	// پیام گیرکرده: worker قبلی بعد از claim کرش کرده است
	if _, err := db.Exec(`
		UPDATE messages SET status = 'sending', updated_at = NOW() - INTERVAL '10 minutes'
		WHERE id = $1
	`, messageID); err != nil {
		t.Fatal(err)
	}

	outcome, err := repo.ClaimMessageForSending(ctx, messageID, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if outcome != ClaimAcquired {
		t.Fatalf("stale sending message must be reclaimable, got %d", outcome)
	}

	// در حالت تازه (بدون کهنه بودن) نباید دوباره claim شود
	outcomeAgain, err := repo.ClaimMessageForSending(ctx, messageID, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if outcomeAgain != ClaimInFlight {
		t.Fatalf("fresh sending message must be in_flight, got %d", outcomeAgain)
	}
}

func TestConsumer_InFlightDoesNotCommitAsDone(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	ctx := context.Background()

	userID := insertTestUser(t, db, 5)
	t.Cleanup(func() { deleteUserCascade(t, db, userID) })
	messageID := seedQueuedMessage(t, db, userID, domain.DeliveryNormal)

	if _, err := db.Exec(`UPDATE messages SET status = 'sending', updated_at = NOW() WHERE id = $1`, messageID); err != nil {
		t.Fatal(err)
	}

	processor := NewMessageProcessor(repo, &countingSender{}, nil, nil)
	err := processor.Process(ctx, smsPayload(t, messageID, "normal"))
	if !errors.Is(err, errRetryLater) {
		t.Fatalf("want errRetryLater, got %v", err)
	}
	if status, _ := messageStatus(t, db, messageID); status != string(domain.MessageSending) {
		t.Fatalf("status=%s, want sending", status)
	}
}

// --- تست اولویت و سهم حداقلی lane‌ها ---

type fakeReader struct {
	records chan kafka.Record
	commits *atomic.Int64
}

func (f *fakeReader) Fetch(ctx context.Context) (kafka.Record, error) {
	select {
	case rec := <-f.records:
		return rec, nil
	case <-ctx.Done():
		return kafka.Record{}, ctx.Err()
	}
}

func (f *fakeReader) Commit(_ context.Context, _ kafka.Record) error {
	f.commits.Add(1)
	return nil
}

func (f *fakeReader) Close() error { return nil }

// slowProcessor پردازش کند را شبیه‌سازی می‌کند تا رقابت lane‌ها دیده شود.
type slowProcessor struct {
	delay   time.Duration
	mu      sync.Mutex
	perLane map[string]int
	done    chan struct{}
	total   int
	seen    int
}

func (p *slowProcessor) Process(_ context.Context, payload []byte) error {
	time.Sleep(p.delay)

	var e struct {
		DeliveryMode string `json:"deliveryMode"`
	}
	if err := json.Unmarshal(payload, &e); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.perLane[e.DeliveryMode]++
	p.seen++
	if p.seen == p.total {
		close(p.done)
	}
	return nil
}

func TestConsumer_NormalNotStarvedByExpressFlood(t *testing.T) {
	const (
		expressCount = 60
		normalCount  = 6
	)

	expressCh := make(chan kafka.Record, expressCount)
	normalCh := make(chan kafka.Record, normalCount)
	for i := 0; i < expressCount; i++ {
		expressCh <- kafka.Record{Topic: "sms.express", Value: []byte(`{"deliveryMode":"express"}`)}
	}
	for i := 0; i < normalCount; i++ {
		normalCh <- kafka.Record{Topic: "sms.normal", Value: []byte(`{"deliveryMode":"normal"}`)}
	}

	var commits atomic.Int64
	newReader := func(topic, _ string) LaneReader {
		if topic == "sms.express" {
			return &fakeReader{records: expressCh, commits: &commits}
		}
		return &fakeReader{records: normalCh, commits: &commits}
	}

	processor := &slowProcessor{
		delay:   2 * time.Millisecond,
		perLane: map[string]int{},
		done:    make(chan struct{}),
		total:   expressCount + normalCount,
	}

	consumer := NewSMSConsumer(
		processor,
		newReader,
		Lane{Name: "express", Topic: "sms.express", GroupID: "g-express", Workers: 6},
		Lane{Name: "normal", Topic: "sms.normal", GroupID: "g-normal", Workers: 2},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go consumer.Run(ctx)

	select {
	case <-processor.done:
	case <-ctx.Done():
		processor.mu.Lock()
		defer processor.mu.Unlock()
		t.Fatalf("timeout: express=%d normal=%d", processor.perLane["express"], processor.perLane["normal"])
	}

	processor.mu.Lock()
	defer processor.mu.Unlock()
	if processor.perLane["express"] != expressCount {
		t.Fatalf("express processed=%d, want %d", processor.perLane["express"], expressCount)
	}
	if processor.perLane["normal"] != normalCount {
		t.Fatalf("normal processed=%d, want %d (normal must not starve)", processor.perLane["normal"], normalCount)
	}
	if commits.Load() != int64(expressCount+normalCount) {
		t.Fatalf("commits=%d, want %d", commits.Load(), expressCount+normalCount)
	}
}

func TestConsumer_ExpressGetsMoreCapacityAndNormalKeepsMinimum(t *testing.T) {
	consumer := NewSMSConsumer(
		&slowProcessor{perLane: map[string]int{}, done: make(chan struct{}), total: 1},
		func(string, string) LaneReader { return nil },
		Lane{Name: "express", Topic: "sms.express", GroupID: "g-express", Workers: 6},
		Lane{Name: "normal", Topic: "sms.normal", GroupID: "g-normal", Workers: 0},
	)

	lanes := consumer.Lanes()
	if len(lanes) != 2 {
		t.Fatalf("lanes=%d, want 2", len(lanes))
	}
	if lanes[0].Name != "express" {
		t.Fatalf("first lane=%s, want express", lanes[0].Name)
	}
	if lanes[1].Workers < 1 {
		t.Fatal("normal lane must keep a guaranteed minimum worker")
	}
	if lanes[0].Workers <= lanes[1].Workers {
		t.Fatalf("express workers=%d must exceed normal workers=%d", lanes[0].Workers, lanes[1].Workers)
	}
}

func TestLogSender_IdempotentByKey(t *testing.T) {
	sender := &LogSender{}
	ctx := context.Background()
	if err := sender.Send(ctx, "msg:1", "09120000001", "hi"); err != nil {
		t.Fatal(err)
	}
	if err := sender.Send(ctx, "msg:1", "09120000001", "hi"); err != nil {
		t.Fatal(err)
	}
	// کلید متفاوت باید ارسال جدید باشد (هر دو بدون خطا)
	if err := sender.Send(ctx, "msg:2", "09120000001", "hi"); err != nil {
		t.Fatal(err)
	}
}

type fakeDLQ struct {
	mu    sync.Mutex
	items [][]byte
}

func (f *fakeDLQ) PublishDLQ(_ context.Context, _ string, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]byte, len(value))
	copy(cp, value)
	f.items = append(f.items, cp)
	return nil
}

func (f *fakeDLQ) len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.items)
}

func TestConsumer_InvalidPayloadGoesToDLQ(t *testing.T) {
	dlq := &fakeDLQ{}
	processor := NewMessageProcessor(NewRepo(openTestDB(t)), &LogSender{}, dlq, nil)
	if err := processor.Process(context.Background(), []byte(`{not-json`)); err != nil {
		t.Fatal(err)
	}
	if dlq.len() != 1 {
		t.Fatalf("dlq=%d, want 1", dlq.len())
	}
}

func TestConsumer_ProviderFailureGoesToDLQ(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	userID := insertTestUser(t, db, 5)
	t.Cleanup(func() { deleteUserCascade(t, db, userID) })
	messageID := seedQueuedMessage(t, db, userID, domain.DeliveryNormal)

	dlq := &fakeDLQ{}
	processor := NewMessageProcessor(repo, &countingSender{fail: true}, dlq, nil)
	if err := processor.Process(context.Background(), smsPayload(t, messageID, "normal")); err != nil {
		t.Fatal(err)
	}
	if status, _ := messageStatus(t, db, messageID); status != string(domain.MessageFailed) {
		t.Fatalf("status=%s, want failed", status)
	}
	if dlq.len() != 1 {
		t.Fatalf("dlq=%d, want 1", dlq.len())
	}
}

package messaging

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/soheil/arvan/internal/messaging/domain"
)

// ─────────────────────────────────────────────────────────────
// Correctness suite — قبل از stress/k6 باید این‌ها سبز باشند.
// مسیر: Send → outbox → (fake Kafka) → MessageProcessor → sent
// بدون وابستگی به Kafka زنده / worker پس‌زمینه.
// ─────────────────────────────────────────────────────────────

func recipientsN(n, seed int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		// ۱۱ رقم عددی معتبر برای isValidMobile
		out[i] = fmt.Sprintf("09%08d", (seed*1000+i)%100_000_000)
	}
	return out
}

func countMsgStatus(t *testing.T, db *sql.DB, userID int64) map[string]int {
	t.Helper()
	rows, err := db.Query(`SELECT status, COUNT(*) FROM messages WHERE user_id = $1 GROUP BY status`, userID)
	if err != nil {
		t.Fatalf("status counts: %v", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			t.Fatal(err)
		}
		out[st] = n
	}
	return out
}

func countOutboxTopics(t *testing.T, db *sql.DB, userID int64) map[string]int {
	t.Helper()
	rows, err := db.Query(`
		SELECT o.topic, COUNT(*)
		FROM outbox_events o
		JOIN messages m ON m.id = o.aggregate_id
		WHERE m.user_id = $1
		GROUP BY o.topic
	`, userID)
	if err != nil {
		t.Fatalf("topic counts: %v", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var topic string
		var n int
		if err := rows.Scan(&topic, &n); err != nil {
			t.Fatal(err)
		}
		out[topic] = n
	}
	return out
}

// drainSendToTerminal outbox کاربر را publish می‌کند و همه payloadها را تا sent/failed می‌رساند.
func drainSendToTerminal(t *testing.T, ctx context.Context, repo *Repo, userID int64, sender SMSSender) *countingSender {
	t.Helper()
	pub := &fakePublisher{}
	processUserPending(t, ctx, repo, pub, userID)

	var cs *countingSender
	if s, ok := sender.(*countingSender); ok {
		cs = s
	} else {
		cs = &countingSender{}
		sender = cs
	}
	processor := NewMessageProcessor(repo, sender, nil, nil)
	for _, msg := range pub.snapshot() {
		if err := processor.Process(ctx, msg.Value); err != nil {
			t.Fatalf("process payload: %v", err)
		}
	}
	return cs
}

func (f *fakePublisher) snapshot() []publishedMsg {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]publishedMsg, len(f.published))
	copy(out, f.published)
	return out
}

func (f *fakePublisher) PublishDLQ(context.Context, string, []byte) error { return nil }

// مرحله ۱ — E2E با اعداد قابل شمارش
func TestCorrectness_E2E_ExactCountsAndAllSent(t *testing.T) {
	const (
		requests   = 100
		perRequest = 10
		wantMsgs   = requests * perRequest // 1000
	)

	db := openTestDB(t)
	repo := NewRepo(db)
	uc := NewUseCase(repo, newTestRedis(t), "sms.normal", "sms.express", nil)
	ctx := context.Background()

	userID := insertTestUser(t, db, int64(wantMsgs)+100)
	t.Cleanup(func() { deleteUserCascade(t, db, userID) })

	for i := 0; i < requests; i++ {
		res, err := uc.Send(ctx, SendMsgRequest{
			UserID:         userID,
			IdempotencyKey: fmt.Sprintf("e2e-%d-%d", time.Now().UnixNano(), i),
			Text: &TextPayload{
				DeliveryMode: "normal",
				Text:         "e2e",
				Recipients:   recipientsN(perRequest, i+1),
			},
		})
		if err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		if res.AcceptedCount != perRequest {
			t.Fatalf("send %d accepted=%d want %d", i, res.AcceptedCount, perRequest)
		}
	}

	if got := countUserRequests(t, userID); got != requests {
		t.Fatalf("requests=%d want %d", got, requests)
	}
	if got := countUserMessages(t, userID); got != wantMsgs {
		t.Fatalf("messages=%d want %d", got, wantMsgs)
	}
	if got := countUserOutbox(t, userID, domain.OutboxPending); got != wantMsgs {
		t.Fatalf("outbox pending=%d want %d", got, wantMsgs)
	}

	sender := &countingSender{}
	drainSendToTerminal(t, ctx, repo, userID, sender)

	if sender.calls.Load() != int64(wantMsgs) {
		t.Fatalf("provider calls=%d want %d (no duplicates)", sender.calls.Load(), wantMsgs)
	}
	if countUserOutbox(t, userID, domain.OutboxPublished) != wantMsgs {
		t.Fatalf("outbox published=%d want %d", countUserOutbox(t, userID, domain.OutboxPublished), wantMsgs)
	}

	st := countMsgStatus(t, db, userID)
	if st[string(domain.MessageSent)] != wantMsgs {
		t.Fatalf("sent=%d want %d; statuses=%v", st[string(domain.MessageSent)], wantMsgs, st)
	}
	if st[string(domain.MessageSending)] != 0 || st[string(domain.MessageQueued)] != 0 || st[string(domain.MessageFailed)] != 0 {
		t.Fatalf("non-terminal leftover: %v", st)
	}
	if bal := readBalance(t, db, userID); bal != 100 {
		t.Fatalf("balance=%d want 100 (started wantMsgs+100)", bal)
	}
}

// مرحله ۲ — موجودی دقیق + concurrency بدون منفی
func TestCorrectness_Balance_ExactDebitThenConcurrentPartial(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	uc := NewUseCase(repo, newTestRedis(t), "sms.normal", "sms.express", nil)
	ctx := context.Background()

	t.Run("exact_debit_to_zero", func(t *testing.T) {
		userID := insertTestUser(t, db, 1000)
		t.Cleanup(func() { deleteUserCascade(t, db, userID) })

		// 100 request × 10 recipient = 1000 SMS
		for i := 0; i < 100; i++ {
			_, err := uc.Send(ctx, SendMsgRequest{
				UserID:         userID,
				IdempotencyKey: fmt.Sprintf("bal-exact-%d-%d", time.Now().UnixNano(), i),
				Text: &TextPayload{
					DeliveryMode: "normal",
					Text:         "b",
					Recipients:   recipientsN(10, 10_000+i),
				},
			})
			if err != nil {
				t.Fatalf("send %d: %v", i, err)
			}
		}
		if bal := readBalance(t, db, userID); bal != 0 {
			t.Fatalf("balance=%d want 0", bal)
		}
		if countUserMessages(t, userID) != 1000 {
			t.Fatalf("messages=%d want 1000", countUserMessages(t, userID))
		}
	})

	t.Run("concurrent_partial_never_negative", func(t *testing.T) {
		const (
			balance   = 100
			conc      = 100
			perReq    = 20
			wantAccept = balance // هر SMS = 1
		)
		userID := insertTestUser(t, db, balance)
		t.Cleanup(func() { deleteUserCascade(t, db, userID) })

		var (
			wg       sync.WaitGroup
			accepted atomic.Int64
			skipped  atomic.Int64
		)
		wg.Add(conc)
		for i := 0; i < conc; i++ {
			i := i
			go func() {
				defer wg.Done()
				res, err := uc.Send(ctx, SendMsgRequest{
					UserID:         userID,
					IdempotencyKey: fmt.Sprintf("bal-conc-%d-%d", time.Now().UnixNano(), i),
					Text: &TextPayload{
						DeliveryMode: "normal",
						Text:         "c",
						Recipients:   recipientsN(perReq, 50_000+i),
					},
				})
				if err != nil {
					t.Errorf("send %d: %v", i, err)
					return
				}
				accepted.Add(int64(res.AcceptedCount))
				skipped.Add(int64(res.SkippedCount + res.RejectedCount))
			}()
		}
		wg.Wait()

		if accepted.Load() != int64(wantAccept) {
			t.Fatalf("accepted SMS=%d want %d", accepted.Load(), wantAccept)
		}
		// 100*20 - 100 = 1900 skipped (insufficient)
		if skipped.Load() != int64(conc*perReq-wantAccept) {
			t.Fatalf("skipped=%d want %d", skipped.Load(), conc*perReq-wantAccept)
		}
		bal := readBalance(t, db, userID)
		if bal != 0 {
			t.Fatalf("balance=%d want 0", bal)
		}
		if bal < 0 {
			t.Fatal("balance must never be negative")
		}
		if countUserMessages(t, userID) != wantAccept {
			t.Fatalf("persisted messages=%d want %d", countUserMessages(t, userID), wantAccept)
		}
	})
}

// مرحله ۳ — کوبیدن idempotency
func TestCorrectness_Idempotency_HammerSameKey(t *testing.T) {
	const (
		hammer     = 100
		recipients = 10
	)
	db := openTestDB(t)
	repo := NewRepo(db)
	uc := NewUseCase(repo, newTestRedis(t), "sms.normal", "sms.express", nil)
	ctx := context.Background()

	userID := insertTestUser(t, db, 500)
	t.Cleanup(func() { deleteUserCascade(t, db, userID) })

	key := fmt.Sprintf("idem-hammer-%d", time.Now().UnixNano())
	cmd := SendMsgRequest{
		UserID:         userID,
		IdempotencyKey: key,
		Text: &TextPayload{
			DeliveryMode: "express",
			Text:         "same",
			Recipients:   recipientsN(recipients, 77),
		},
	}

	var (
		wg       sync.WaitGroup
		okCount  atomic.Int64
		reqID    atomic.Int64
		firstSet sync.Once
	)
	wg.Add(hammer)
	for i := 0; i < hammer; i++ {
		go func() {
			defer wg.Done()
			res, err := uc.Send(ctx, cmd)
			if err != nil {
				t.Errorf("send: %v", err)
				return
			}
			okCount.Add(1)
			firstSet.Do(func() { reqID.Store(res.RequestID) })
			if res.RequestID == 0 || res.AcceptedCount != recipients {
				t.Errorf("bad result: %+v", res)
			}
			if got := reqID.Load(); got != 0 && res.RequestID != got {
				t.Errorf("requestId mismatch got=%d want=%d", res.RequestID, got)
			}
		}()
	}
	wg.Wait()

	if okCount.Load() != hammer {
		t.Fatalf("successful responses=%d want %d", okCount.Load(), hammer)
	}
	if countUserRequests(t, userID) != 1 {
		t.Fatalf("requests=%d want 1", countUserRequests(t, userID))
	}
	if countUserMessages(t, userID) != recipients {
		t.Fatalf("messages=%d want %d", countUserMessages(t, userID), recipients)
	}
	if bal := readBalance(t, db, userID); bal != 500-int64(recipients) {
		t.Fatalf("balance=%d want %d (single debit)", bal, 500-recipients)
	}
}

// مرحله ۴ — تفکیک express / normal
func TestCorrectness_ExpressNormal_SeparateTopicsAndBothDrain(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	ctx := context.Background()

	t.Run("normal_heavy", func(t *testing.T) {
		// 100 express + 1000 normal
		uid := insertTestUser(t, db, 5000)
		t.Cleanup(func() { deleteUserCascade(t, db, uid) })
		uc2 := NewUseCase(repo, newTestRedis(t), "sms.normal", "sms.express", nil)

		for i := 0; i < 100; i++ {
			_, err := uc2.Send(ctx, SendMsgRequest{
				UserID: uid, IdempotencyKey: fmt.Sprintf("ex-%d", i),
				Text: &TextPayload{DeliveryMode: "express", Text: "e", Recipients: recipientsN(1, 1000+i)},
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 100; i++ {
			_, err := uc2.Send(ctx, SendMsgRequest{
				UserID: uid, IdempotencyKey: fmt.Sprintf("no-%d", i),
				Text: &TextPayload{DeliveryMode: "normal", Text: "n", Recipients: recipientsN(10, 2000+i)},
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		topics := countOutboxTopics(t, db, uid)
		if topics["sms.express"] != 100 {
			t.Fatalf("express outbox=%d want 100 (%v)", topics["sms.express"], topics)
		}
		if topics["sms.normal"] != 1000 {
			t.Fatalf("normal outbox=%d want 1000 (%v)", topics["sms.normal"], topics)
		}

		sender := &countingSender{}
		drainSendToTerminal(t, ctx, repo, uid, sender)
		st := countMsgStatus(t, db, uid)
		if st[string(domain.MessageSent)] != 1100 {
			t.Fatalf("sent=%d want 1100 %v", st[string(domain.MessageSent)], st)
		}
	})

	t.Run("express_heavy", func(t *testing.T) {
		// 100 normal + 1000 express
		uid := insertTestUser(t, db, 5000)
		t.Cleanup(func() { deleteUserCascade(t, db, uid) })
		uc2 := NewUseCase(repo, newTestRedis(t), "sms.normal", "sms.express", nil)

		for i := 0; i < 100; i++ {
			_, err := uc2.Send(ctx, SendMsgRequest{
				UserID: uid, IdempotencyKey: fmt.Sprintf("n2-%d", i),
				Text: &TextPayload{DeliveryMode: "normal", Text: "n", Recipients: recipientsN(1, 3000+i)},
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 100; i++ {
			_, err := uc2.Send(ctx, SendMsgRequest{
				UserID: uid, IdempotencyKey: fmt.Sprintf("e2-%d", i),
				Text: &TextPayload{DeliveryMode: "express", Text: "e", Recipients: recipientsN(10, 4000+i)},
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		topics := countOutboxTopics(t, db, uid)
		if topics["sms.normal"] != 100 || topics["sms.express"] != 1000 {
			t.Fatalf("topics=%v want normal=100 express=1000", topics)
		}
		drainSendToTerminal(t, ctx, repo, uid, &countingSender{})
		if countMsgStatus(t, db, uid)[string(domain.MessageSent)] != 1100 {
			t.Fatalf("sent statuses=%v", countMsgStatus(t, db, uid))
		}
	})
}

// مرحله ۵ — Failure injection
func TestCorrectness_FailureInjection_KafkaThenProvider(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	uc := NewUseCase(repo, newTestRedis(t), "sms.normal", "sms.express", nil)
	ctx := context.Background()

	t.Run("kafka_down_keeps_outbox_pending_then_recovers", func(t *testing.T) {
		userID := insertTestUser(t, db, 50)
		t.Cleanup(func() { deleteUserCascade(t, db, userID) })

		_, err := uc.Send(ctx, SendMsgRequest{
			UserID:         userID,
			IdempotencyKey: fmt.Sprintf("kafka-down-%d", time.Now().UnixNano()),
			Text: &TextPayload{
				DeliveryMode: "normal",
				Text:         "k",
				Recipients:   recipientsN(5, 900),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if countUserOutbox(t, userID, domain.OutboxPending) != 5 {
			t.Fatal("after Send, outbox must be pending (HTTP succeeded without Kafka)")
		}
		if st := countMsgStatus(t, db, userID); st[string(domain.MessageQueued)] != 5 {
			t.Fatalf("messages should stay queued until consumer: %v", st)
		}

		failPub := &fakePublisher{fail: true}
		processUserPending(t, ctx, repo, failPub, userID)
		if countUserOutbox(t, userID, domain.OutboxPending) != 5 {
			t.Fatal("with Kafka down, outbox must remain pending")
		}

		okPub := &fakePublisher{}
		processUserPending(t, ctx, repo, okPub, userID)
		if okPub.count() != 5 || countUserOutbox(t, userID, domain.OutboxPublished) != 5 {
			t.Fatalf("after Kafka recovery published=%d count=%d", countUserOutbox(t, userID, domain.OutboxPublished), okPub.count())
		}

		processor := NewMessageProcessor(repo, &countingSender{}, nil, nil)
		for _, m := range okPub.snapshot() {
			if err := processor.Process(ctx, m.Value); err != nil {
				t.Fatal(err)
			}
		}
		st := countMsgStatus(t, db, userID)
		if st[string(domain.MessageSent)] != 5 || st[string(domain.MessageQueued)] != 0 || st[string(domain.MessageSending)] != 0 {
			t.Fatalf("after recovery statuses=%v", st)
		}
	})

	t.Run("provider_fail_ends_failed_not_stuck_sending", func(t *testing.T) {
		userID := insertTestUser(t, db, 50)
		t.Cleanup(func() { deleteUserCascade(t, db, userID) })

		_, err := uc.Send(ctx, SendMsgRequest{
			UserID:         userID,
			IdempotencyKey: fmt.Sprintf("prov-fail-%d", time.Now().UnixNano()),
			Text: &TextPayload{
				DeliveryMode: "express",
				Text:         "f",
				Recipients:   recipientsN(3, 901),
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		pub := &fakePublisher{}
		processUserPending(t, ctx, repo, pub, userID)
		processor := NewMessageProcessor(repo, &countingSender{fail: true}, &fakeDLQ{}, nil)
		for _, m := range pub.snapshot() {
			if err := processor.Process(ctx, m.Value); err != nil {
				t.Fatal(err)
			}
		}
		st := countMsgStatus(t, db, userID)
		if st[string(domain.MessageFailed)] != 3 {
			t.Fatalf("want 3 failed, got %v", st)
		}
		if st[string(domain.MessageSending)] != 0 {
			t.Fatalf("must not stay in sending: %v", st)
		}
	})
}

// مرحله ۶ — تحویل تکراری Kafka
func TestCorrectness_DuplicateDelivery_ProviderOnce(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	uc := NewUseCase(repo, newTestRedis(t), "sms.normal", "sms.express", nil)
	ctx := context.Background()

	userID := insertTestUser(t, db, 10)
	t.Cleanup(func() { deleteUserCascade(t, db, userID) })

	res, err := uc.Send(ctx, SendMsgRequest{
		UserID:         userID,
		IdempotencyKey: fmt.Sprintf("dup-%d", time.Now().UnixNano()),
		Text: &TextPayload{
			DeliveryMode: "normal",
			Text:         "d",
			Recipients:   recipientsN(1, 42),
		},
	})
	if err != nil || res.AcceptedCount != 1 {
		t.Fatalf("send: %v result=%+v", err, res)
	}

	pub := &fakePublisher{}
	processUserPending(t, ctx, repo, pub, userID)
	if pub.count() != 1 {
		t.Fatalf("published=%d", pub.count())
	}
	payload := pub.snapshot()[0].Value

	sender := &countingSender{}
	processor := NewMessageProcessor(repo, sender, nil, nil)
	for i := 0; i < 3; i++ {
		if err := processor.Process(ctx, payload); err != nil {
			t.Fatalf("delivery %d: %v", i+1, err)
		}
	}
	if sender.calls.Load() != 1 {
		t.Fatalf("provider invokes=%d want 1", sender.calls.Load())
	}
	st := countMsgStatus(t, db, userID)
	if st[string(domain.MessageSent)] != 1 {
		t.Fatalf("statuses=%v", st)
	}
}

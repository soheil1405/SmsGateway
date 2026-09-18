package messaging

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5433/arvan?sslmode=disable"
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Skipf("cannot open postgres: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Skipf("postgres unavailable (set TEST_DATABASE_URL or start docker-compose): %v", err)
	}

	ensureTestSchema(t, db)

	db.SetMaxOpenConns(40)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// ensureTestSchema migrationهای لازم برای volumeهای از قبل ساخته‌شده را اعمال می‌کند.
func ensureTestSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`ALTER TABLE outbox_events DROP CONSTRAINT IF EXISTS outbox_events_status_check`,
		`ALTER TABLE outbox_events ADD CONSTRAINT outbox_events_status_check CHECK (status IN ('pending', 'publishing', 'published', 'failed'))`,
		`ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ`,
		`DROP TABLE IF EXISTS balance_reservations`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("ensure schema: %v\nstmt: %s", err, s)
		}
	}
}

func insertTestUser(t *testing.T, db *sql.DB, balance int64) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(`
		INSERT INTO users (name, balance)
		VALUES ($1, $2)
		RETURNING id
	`, fmt.Sprintf("conc-%d", time.Now().UnixNano()), balance).Scan(&id)
	if err != nil {
		t.Fatalf("insert test user: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM users WHERE id = $1`, id)
	})
	return id
}

func readBalance(t *testing.T, db *sql.DB, userID int64) int64 {
	t.Helper()
	var bal int64
	if err := db.QueryRow(`SELECT balance FROM users WHERE id = $1`, userID).Scan(&bal); err != nil {
		t.Fatalf("read balance: %v", err)
	}
	return bal
}

// debitOnce همان الگوی Send را شبیه‌سازی می‌کند:
// BEGIN → FOR UPDATE → planSend → DebitBalance → COMMIT
func debitOnce(ctx context.Context, repo *Repo, userID int64, recipients []string) (debited int64, err error) {
	tx, err := repo.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	balance, err := repo.GetBalanceForUpdate(ctx, tx, userID)
	if err != nil {
		return 0, err
	}

	planned := planSend(SendMsgRequest{
		UserID:         userID,
		IdempotencyKey: fmt.Sprintf("k-%d", time.Now().UnixNano()),
		Text: &TextPayload{
			DeliveryMode: "normal",
			Text:         "hi",
			Recipients:   recipients,
		},
	}, balance)

	if err := repo.DebitBalance(ctx, tx, userID, planned.TotalCost); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return planned.TotalCost, nil
}

func TestDebitBalance_ConcurrentNeverNegative(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	ctx := context.Background()

	const (
		initialBalance int64 = 100
		costEach       int64 = 10
		workers              = 50
	)

	userID := insertTestUser(t, db, initialBalance)
	recipients := make([]string, costEach) // هر request می‌خواهد costEach تومان مصرف کند
	for i := range recipients {
		recipients[i] = fmt.Sprintf("091200000%02d", i)
	}

	var (
		wg           sync.WaitGroup
		success      atomic.Int64
		totalDebited atomic.Int64
	)

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			debited, err := debitOnce(ctx, repo, userID, recipients)
			if err != nil {
				t.Errorf("debitOnce: %v", err)
				return
			}
			if debited > 0 {
				success.Add(1)
				totalDebited.Add(debited)
			}
		}()
	}
	wg.Wait()

	final := readBalance(t, db, userID)

	if final < 0 {
		t.Fatalf("balance went negative: %d", final)
	}
	if final != 0 {
		t.Fatalf("expected final balance 0, got %d (debited=%d success=%d)", final, totalDebited.Load(), success.Load())
	}
	if totalDebited.Load() != initialBalance {
		t.Fatalf("total debited=%d, want %d", totalDebited.Load(), initialBalance)
	}
	if initialBalance-totalDebited.Load() != final {
		t.Fatalf("accounting broken: initial(%d) - debited(%d) != final(%d)", initialBalance, totalDebited.Load(), final)
	}
	maxOK := initialBalance / costEach
	if success.Load() != maxOK {
		t.Fatalf("successful debits=%d, want %d", success.Load(), maxOK)
	}
}

func TestDebitBalance_ConcurrentPartialAcceptNeverNegative(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepo(db)
	ctx := context.Background()

	const (
		initialBalance int64 = 50
		workers              = 30
		recipientsN          = 100 // بیشتر از موجودی → پذیرش جزئی
	)

	userID := insertTestUser(t, db, initialBalance)
	recipients := make([]string, recipientsN)
	for i := range recipients {
		recipients[i] = fmt.Sprintf("0912111%04d", i)
	}

	var (
		wg           sync.WaitGroup
		totalDebited atomic.Int64
	)

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			debited, err := debitOnce(ctx, repo, userID, recipients)
			if err != nil {
				t.Errorf("debitOnce: %v", err)
				return
			}
			totalDebited.Add(debited)
		}()
	}
	wg.Wait()

	final := readBalance(t, db, userID)

	if final < 0 {
		t.Fatalf("balance went negative: %d", final)
	}
	if final != 0 {
		t.Fatalf("expected final balance 0, got %d", final)
	}
	if totalDebited.Load() != initialBalance {
		t.Fatalf("total debited=%d, want exactly initial balance %d (no oversell)", totalDebited.Load(), initialBalance)
	}
}

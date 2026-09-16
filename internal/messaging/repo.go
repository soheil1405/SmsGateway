package messaging

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/soheil/arvan/internal/messaging/domain"
)

// خطاهای لایهٔ repository ماژول messaging
var (
	ErrNotFound            = errors.New("not found")
	ErrInsufficientBalance = errors.New("insufficient balance")
	ErrConflict            = errors.New("conflict")
)

// Repo دسترسی به جداول messaging در Postgres است.
type Repo struct {
	db *sql.DB
}

// NewRepo یک repository روی اتصال دیتابیس می‌سازد.
func NewRepo(db *sql.DB) *Repo {
	return &Repo{db: db}
}

// Begin یک تراکنش جدید شروع می‌کند.
func (r *Repo) Begin(ctx context.Context) (*sql.Tx, error) {
	return r.db.BeginTx(ctx, nil)
}

// FindRequestByIdempotency درخواست قبلی با همان کلید را پیدا می‌کند.
func (r *Repo) FindRequestByIdempotency(ctx context.Context, userID int64, key string) (*domain.Request, error) {
	req := &domain.Request{}
	var response []byte
	err := r.db.QueryRowContext(ctx, `
		SELECT id, user_id, idempotency_key, payload_hash, total_cost,
		       accepted_count, rejected_count, response_json, created_at
		FROM requests
		WHERE user_id = $1 AND idempotency_key = $2
	`, userID, key).Scan(
		&req.ID, &req.UserID, &req.IdempotencyKey, &req.PayloadHash, &req.TotalCost,
		&req.AcceptedCount, &req.RejectedCount, &response, &req.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	req.ResponseJSON = response
	return req, nil
}

// CreateRequest یک رکورد request جدید درج می‌کند.
// در صورت تکراری بودن کلید idempotency، ErrConflict برمی‌گرداند.
func (r *Repo) CreateRequest(ctx context.Context, tx *sql.Tx, req *domain.Request) error {
	err := tx.QueryRowContext(ctx, `
		INSERT INTO requests (user_id, idempotency_key, payload_hash, total_cost, accepted_count, rejected_count, response_json)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, created_at
	`, req.UserID, req.IdempotencyKey, req.PayloadHash, req.TotalCost,
		req.AcceptedCount, req.RejectedCount, req.ResponseJSON,
	).Scan(&req.ID, &req.CreatedAt)
	if isUniqueViolation(err) {
		return ErrConflict
	}
	return err
}

// UpdateRequestResponse آمار و اسنپ‌شات پاسخ را روی request به‌روز می‌کند.
func (r *Repo) UpdateRequestResponse(ctx context.Context, tx *sql.Tx, id int64, accepted, rejected int, totalCost int64, response json.RawMessage) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE requests
		SET accepted_count = $2, rejected_count = $3, total_cost = $4, response_json = $5
		WHERE id = $1
	`, id, accepted, rejected, totalCost, response)
	return err
}

// CreateReservation رزرو موجودی در جدول balance_reservations را ثبت می‌کند.
func (r *Repo) CreateReservation(ctx context.Context, tx *sql.Tx, res *domain.BalanceReservation) error {
	return tx.QueryRowContext(ctx, `
		INSERT INTO balance_reservations (user_id, request_id, amount, status, expires_at)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, created_at
	`, res.UserID, res.RequestID, res.Amount, res.Status, res.ExpiresAt,
	).Scan(&res.ID, &res.CreatedAt)
}

// CommitReservation وضعیت رزرو را از pending به committed می‌برد.
func (r *Repo) CommitReservation(ctx context.Context, tx *sql.Tx, requestID int64) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE balance_reservations SET status = $2 WHERE request_id = $1 AND status = $3
	`, requestID, domain.ReservationCommitted, domain.ReservationPending)
	return err
}

// CreateMessage یک پیام پذیرفته‌شده را در جدول messages درج می‌کند.
func (r *Repo) CreateMessage(ctx context.Context, tx *sql.Tx, m *domain.Message) error {
	return tx.QueryRowContext(ctx, `
		INSERT INTO messages (request_id, user_id, recipient, type, delivery_mode, text, status, cost, error_code)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id, created_at, updated_at
	`, m.RequestID, m.UserID, m.Recipient, m.Type, m.DeliveryMode, m.Text, m.Status, m.Cost, m.ErrorCode,
	).Scan(&m.ID, &m.CreatedAt, &m.UpdatedAt)
}

// CreateOutbox رویداد outbox را برای انتشار بعدی ثبت می‌کند.
func (r *Repo) CreateOutbox(ctx context.Context, tx *sql.Tx, e *domain.OutboxEvent) error {
	return tx.QueryRowContext(ctx, `
		INSERT INTO outbox_events (aggregate_id, topic, partition_key, payload, status)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, created_at
	`, e.AggregateID, e.Topic, e.PartitionKey, e.Payload, e.Status,
	).Scan(&e.ID, &e.CreatedAt)
}

// ListMessages پیام‌ها را با فیلترهای اختیاری برمی‌گرداند.
func (r *Repo) ListMessages(ctx context.Context, filter MessageFilter) ([]domain.Message, error) {
	query := `
		SELECT id, request_id, user_id, recipient, type, delivery_mode, text, status, cost, error_code, created_at, updated_at
		FROM messages
		WHERE 1=1`
	args := make([]any, 0, 7)
	n := 1

	// ساخت پویای شرط‌های WHERE بر اساس فیلتر
	if filter.UserID != nil {
		query += fmt.Sprintf(" AND user_id = $%d", n)
		args = append(args, *filter.UserID)
		n++
	}
	if filter.RequestID != nil {
		query += fmt.Sprintf(" AND request_id = $%d", n)
		args = append(args, *filter.RequestID)
		n++
	}
	if filter.Status != "" {
		query += fmt.Sprintf(" AND status = $%d", n)
		args = append(args, filter.Status)
		n++
	}
	if filter.Type != "" {
		query += fmt.Sprintf(" AND type = $%d", n)
		args = append(args, filter.Type)
		n++
	}
	if filter.DeliveryMode != "" {
		query += fmt.Sprintf(" AND delivery_mode = $%d", n)
		args = append(args, filter.DeliveryMode)
		n++
	}
	if filter.Recipient != "" {
		query += fmt.Sprintf(" AND recipient = $%d", n)
		args = append(args, filter.Recipient)
		n++
	}
	if filter.ErrorCode != "" {
		query += fmt.Sprintf(" AND error_code = $%d", n)
		args = append(args, filter.ErrorCode)
	}

	query += " ORDER BY id DESC"

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := make([]domain.Message, 0)
	for rows.Next() {
		var m domain.Message
		if err := rows.Scan(
			&m.ID, &m.RequestID, &m.UserID, &m.Recipient, &m.Type, &m.DeliveryMode,
			&m.Text, &m.Status, &m.Cost, &m.ErrorCode, &m.CreatedAt, &m.UpdatedAt,
		); err != nil {
			return nil, err
		}
		list = append(list, m)
	}
	return list, rows.Err()
}

// GetBalanceForUpdate موجودی کاربر را با قفل ردیف می‌خواند (داخل تراکنش ارسال).
func (r *Repo) GetBalanceForUpdate(ctx context.Context, tx *sql.Tx, userID int64) (int64, error) {
	var balance int64
	err := tx.QueryRowContext(ctx, `
		SELECT balance FROM users WHERE id = $1 FOR UPDATE
	`, userID).Scan(&balance)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	return balance, err
}

// DebitBalance مبلغ را از موجودی کم می‌کند؛ فقط اگر موجودی کافی باشد.
func (r *Repo) DebitBalance(ctx context.Context, tx *sql.Tx, userID, amount int64) error {
	if amount <= 0 {
		return nil
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE users
		SET balance = balance - $2, updated_at = NOW()
		WHERE id = $1 AND balance >= $2
	`, userID, amount)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrInsufficientBalance
	}
	return nil
}

// DefaultReservationExpiry زمان انقضای پیش‌فرض رزرو موجودی است.
func DefaultReservationExpiry() time.Time {
	return time.Now().Add(15 * time.Minute)
}

// isUniqueViolation تشخیص خطای یکتایی Postgres (کد 23505) است.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

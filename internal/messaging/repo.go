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

// ClaimPendingOutbox یک batch را اتمیک از pending (یا publishing کهنه) به publishing می‌برد.
// FOR UPDATE SKIP LOCKED جلوی پردازش موازی چند instance را می‌گیرد.
func (r *Repo) ClaimPendingOutbox(ctx context.Context, limit int, staleAfter time.Duration) ([]domain.OutboxEvent, error) {
	if limit <= 0 {
		limit = 50
	}
	if staleAfter <= 0 {
		staleAfter = time.Minute
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		WITH cte AS (
			SELECT id
			FROM outbox_events
			WHERE status = $1
			   OR (status = $2 AND locked_at < NOW() - make_interval(secs => $3))
			ORDER BY id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $4
		)
		UPDATE outbox_events e
		SET status = $2,
		    locked_at = NOW(),
		    attempts = e.attempts + 1
		FROM cte
		WHERE e.id = cte.id
		RETURNING e.id, e.aggregate_id, e.topic, e.partition_key, e.payload,
		          e.status, e.attempts, e.last_error, e.created_at, e.published_at
	`, domain.OutboxPending, domain.OutboxPublishing, staleAfter.Seconds(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]domain.OutboxEvent, 0, limit)
	for rows.Next() {
		e, err := scanOutboxEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

type outboxScanner interface {
	Scan(dest ...any) error
}

func scanOutboxEvent(row outboxScanner) (domain.OutboxEvent, error) {
	var e domain.OutboxEvent
	var lastErr sql.NullString
	var publishedAt sql.NullTime
	if err := row.Scan(
		&e.ID, &e.AggregateID, &e.Topic, &e.PartitionKey, &e.Payload,
		&e.Status, &e.Attempts, &lastErr, &e.CreatedAt, &publishedAt,
	); err != nil {
		return e, err
	}
	if lastErr.Valid {
		s := lastErr.String
		e.LastError = &s
	}
	if publishedAt.Valid {
		t := publishedAt.Time
		e.PublishedAt = &t
	}
	return e, nil
}

// MarkOutboxPublished وضعیت رویداد را بعد از publish موفق به published می‌برد.
func (r *Repo) MarkOutboxPublished(ctx context.Context, id int64) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE outbox_events
		SET status = $2, published_at = NOW(), last_error = NULL, locked_at = NULL
		WHERE id = $1 AND status IN ($3, $4)
	`, id, domain.OutboxPublished, domain.OutboxPending, domain.OutboxPublishing)
	return err
}

// MarkOutboxFailed رویداد را پس از اتمام تلاش‌ها به failed می‌برد (دیگر claim نمی‌شود).
func (r *Repo) MarkOutboxFailed(ctx context.Context, id int64, errMsg string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE outbox_events
		SET status = $2, last_error = $3, locked_at = NULL
		WHERE id = $1 AND status IN ($4, $5)
	`, id, domain.OutboxFailed, errMsg, domain.OutboxPending, domain.OutboxPublishing)
	return err
}

// RecordOutboxFailure رویداد را به pending برمی‌گرداند تا دوباره claim شود.
func (r *Repo) RecordOutboxFailure(ctx context.Context, id int64, errMsg string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE outbox_events
		SET status = $2, last_error = $3, locked_at = NULL
		WHERE id = $1 AND status IN ($2, $4)
	`, id, domain.OutboxPending, errMsg, domain.OutboxPublishing)
	return err
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

// ClaimOutcome نتیجهٔ تلاش برای تصاحب پیام جهت ارسال است.
type ClaimOutcome int

const (
	ClaimAcquired      ClaimOutcome = iota // تازه claim شد → باید ارسال شود
	ClaimAlreadySent                       // قبلاً sent → offset را commit کن
	ClaimAlreadyFailed                     // قبلاً failed → offset را commit کن
	ClaimInFlight                          // در حال ارسال توسط worker دیگر → commit نکن
	ClaimNotFound                          // پیام وجود ندارد → کنار بگذار
)

// ClaimMessageForSending پیام را از queued (یا sending کهنه) به sending می‌برد.
func (r *Repo) ClaimMessageForSending(ctx context.Context, id int64, staleAfter time.Duration) (ClaimOutcome, error) {
	res, err := r.db.ExecContext(ctx, `
		UPDATE messages
		SET status = $2, updated_at = NOW()
		WHERE id = $1
		  AND (
		        status = $3
		        OR (status = $2 AND updated_at < NOW() - make_interval(secs => $4))
		      )
	`, id, domain.MessageSending, domain.MessageQueued, staleAfter.Seconds())
	if err != nil {
		return ClaimNotFound, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return ClaimNotFound, err
	}
	if n > 0 {
		return ClaimAcquired, nil
	}

	var status string
	err = r.db.QueryRowContext(ctx, `SELECT status FROM messages WHERE id = $1`, id).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return ClaimNotFound, nil
	}
	if err != nil {
		return ClaimNotFound, err
	}
	switch domain.MessageStatus(status) {
	case domain.MessageSent:
		return ClaimAlreadySent, nil
	case domain.MessageFailed:
		return ClaimAlreadyFailed, nil
	case domain.MessageSending:
		return ClaimInFlight, nil
	default:
		// queued که همزمان claim نشده — یا وضعیت غیرمنتظره
		return ClaimInFlight, nil
	}
}

// MarkMessageSent وضعیت پیام را به sent تغییر می‌دهد.
func (r *Repo) MarkMessageSent(ctx context.Context, id int64) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE messages
		SET status = $2, error_code = NULL, updated_at = NOW()
		WHERE id = $1
	`, id, domain.MessageSent)
	return err
}

// MarkMessageFailed وضعیت پیام را با کد خطا به failed تغییر می‌دهد.
func (r *Repo) MarkMessageFailed(ctx context.Context, id int64, code string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE messages
		SET status = $2, error_code = $3, updated_at = NOW()
		WHERE id = $1
	`, id, domain.MessageFailed, code)
	return err
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

// isUniqueViolation تشخیص خطای یکتایی Postgres (کد 23505) است.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

package user

import (
	"context"
	"database/sql"
	"errors"

	"github.com/soheil/arvan/internal/user/domain"
)

// ErrNotFound وقتی کاربر در دیتابیس پیدا نشود برمی‌گردد.
var ErrNotFound = errors.New("user not found")

// Repo دسترسی به جدول users است.
type Repo struct {
	db *sql.DB
}

// NewRepo یک repository کاربر روی اتصال دیتابیس می‌سازد.
func NewRepo(db *sql.DB) *Repo {
	return &Repo{db: db}
}

// GetByID کاربر را با شناسه می‌خواند.
func (r *Repo) GetByID(ctx context.Context, id int64) (*domain.User, error) {
	u := &domain.User{}
	err := r.db.QueryRowContext(ctx, `
		SELECT id, name, balance, created_at, updated_at
		FROM users WHERE id = $1
	`, id).Scan(&u.ID, &u.Name, &u.Balance, &u.CreatedAt, &u.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return u, nil
}

// AddBalance موجودی را افزایش می‌دهد و رکورد به‌روزشده را برمی‌گرداند.
func (r *Repo) AddBalance(ctx context.Context, id, amount int64) (*domain.User, error) {
	u := &domain.User{}
	err := r.db.QueryRowContext(ctx, `
		UPDATE users
		SET balance = balance + $2, updated_at = NOW()
		WHERE id = $1
		RETURNING id, name, balance, created_at, updated_at
	`, id, amount).Scan(&u.ID, &u.Name, &u.Balance, &u.CreatedAt, &u.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return u, nil
}

package user

import (
	"context"
	"database/sql"
	"errors"

	"github.com/soheil/arvan/internal/user/domain"
)

var ErrNotFound = errors.New("user not found")

type Repo struct {
	db *sql.DB
}

func NewRepo(db *sql.DB) *Repo {
	return &Repo{db: db}
}

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

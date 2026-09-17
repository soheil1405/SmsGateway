package user

import (
	"context"
	"errors"

	"github.com/soheil/arvan/internal/user/domain"
	"github.com/soheil/arvan/utils/errs"
)

// UseCase منطق کسب‌وکار کاربر (خواندن و افزایش موجودی) است.
type UseCase struct {
	repo *Repo
}

// NewUseCase یک نمونه UseCase کاربر می‌سازد.
func NewUseCase(repo *Repo) *UseCase {
	return &UseCase{repo: repo}
}

// Get کاربر را با شناسه برمی‌گرداند.
func (uc *UseCase) Get(ctx context.Context, id int64) (*domain.User, error) {
	user, err := uc.repo.GetByID(ctx, id)
	if err != nil {
		return nil, mapRepoErr(err)
	}
	return user, nil
}

// AddBalance مبلغ را به موجودی کاربر اضافه می‌کند.
func (uc *UseCase) AddBalance(ctx context.Context, cmd AddBalanceCommand) (*domain.User, error) {
	user, err := uc.repo.AddBalance(ctx, cmd.ID, cmd.Amount)
	if err != nil {
		return nil, mapRepoErr(err)
	}
	return user, nil
}

// mapRepoErr خطای repository را به خطای مشترک API نگاشت می‌کند.
func mapRepoErr(err error) error {
	if errors.Is(err, ErrNotFound) {
		return errs.ErrNotFound
	}
	return err
}

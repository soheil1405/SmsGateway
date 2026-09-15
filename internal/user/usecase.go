package user

import (
	"context"
	"errors"

	"github.com/soheil/arvan/internal/user/domain"
	"github.com/soheil/arvan/utils/errs"
)

type UseCase struct {
	repo *Repo
}

func NewUseCase(repo *Repo) *UseCase {
	return &UseCase{repo: repo}
}

func (uc *UseCase) Get(ctx context.Context, id int64) (*domain.User, error) {
	user, err := uc.repo.GetByID(ctx, id)
	if err != nil {
		return nil, mapRepoErr(err)
	}
	return user, nil
}

func (uc *UseCase) AddBalance(ctx context.Context, cmd AddBalanceCommand) (*domain.User, error) {
	user, err := uc.repo.AddBalance(ctx, cmd.ID, cmd.Amount)
	if err != nil {
		return nil, mapRepoErr(err)
	}
	return user, nil
}

func mapRepoErr(err error) error {
	if errors.Is(err, ErrNotFound) {
		return errs.ErrNotFound
	}
	return err
}

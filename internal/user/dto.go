package user

import (
	"time"

	"github.com/soheil/arvan/internal/user/domain"
	"github.com/soheil/arvan/utils/errs"
)

type addBalanceRequest struct {
	Amount int64 `json:"amount"`
}

func (r addBalanceRequest) validate() error {
	v := &errs.Validation{}
	if r.Amount <= 0 {
		v.Add("amount", "must be > 0")
	}
	return v.Err()
}

type userResponse struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Balance   int64     `json:"balance"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

func toUserResponse(u *domain.User) userResponse {
	return userResponse{
		ID:        u.ID,
		Name:      u.Name,
		Balance:   u.Balance,
		CreatedAt: u.CreatedAt,
		UpdatedAt: u.UpdatedAt,
	}
}

type AddBalanceCommand struct {
	ID     int64
	Amount int64
}

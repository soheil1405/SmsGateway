package user

import (
	"github.com/labstack/echo/v4"

	"github.com/soheil/arvan/utils/response"
)

type Handler struct {
	uc *UseCase
}

func NewHandler(uc *UseCase) *Handler {
	return &Handler{uc: uc}
}

func (h *Handler) Register(e *echo.Echo) {
	g := e.Group("/users")
	g.GET("/:id", h.Get)
	g.POST("/:id/add-balance", h.AddBalance)
}

func (h *Handler) Get(c echo.Context) error {
	id, err := response.PathID(c, "id")
	if err != nil {
		return err
	}

	user, err := h.uc.Get(c.Request().Context(), id)
	if err != nil {
		return response.FromError(c, err)
	}

	return response.OK(c, toUserResponse(user))
}

func (h *Handler) AddBalance(c echo.Context) error {
	id, err := response.PathID(c, "id")
	if err != nil {
		return err
	}

	var req addBalanceRequest
	if err := response.Bind(c, &req); err != nil {
		return err
	}
	if err := req.validate(); err != nil {
		return response.FromError(c, err)
	}

	user, err := h.uc.AddBalance(c.Request().Context(), AddBalanceCommand{
		ID:     id,
		Amount: req.Amount,
	})
	if err != nil {
		return response.FromError(c, err)
	}

	return response.OK(c, toUserResponse(user))
}

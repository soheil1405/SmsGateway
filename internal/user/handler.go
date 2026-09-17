package user

import (
	"github.com/labstack/echo/v4"

	"github.com/soheil/arvan/utils/response"
)

// Handler لایهٔ HTTP ماژول کاربر است.
type Handler struct {
	uc *UseCase
}

// NewHandler هندلر کاربر را می‌سازد.
func NewHandler(uc *UseCase) *Handler {
	return &Handler{uc: uc}
}

// Register مسیرهای مربوط به کاربر را ثبت می‌کند.
func (h *Handler) Register(e *echo.Echo) {
	g := e.Group("/users")
	g.GET("/:id", h.Get)
	g.POST("/:id/add-balance", h.AddBalance)
}

// Get اطلاعات یک کاربر را برمی‌گرداند.
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

// AddBalance موجودی کاربر را افزایش می‌دهد.
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

package messaging

import (
	"github.com/labstack/echo/v4"

	"github.com/soheil/arvan/utils/response"
)

// Handler لایهٔ HTTP ماژول messaging است.
type Handler struct {
	uc *UseCase
}

// NewHandler هندلر را با UseCase می‌سازد.
func NewHandler(uc *UseCase) *Handler {
	return &Handler{uc: uc}
}

// Register مسیرهای HTTP مربوط به پیامک را ثبت می‌کند.
func (h *Handler) Register(e *echo.Echo) {
	e.POST("/messages/send/otp", h.SendOTP)
	e.POST("/messages/send/text", h.SendText)
	e.GET("/messages", h.ListMessages)
}

// SendOTP ارسال OTP به چند گیرنده را انجام می‌دهد.
func (h *Handler) SendOTP(c echo.Context) error {
	var req sendOTPRequest
	if err := response.Bind(c, &req); err != nil {
		return err
	}
	if err := req.validate(); err != nil {
		return response.FromError(c, err)
	}

	result, err := h.uc.Send(c.Request().Context(), req.toCommand())
	if err != nil {
		return response.FromError(c, err)
	}
	return response.OK(c, result)
}

// SendText ارسال پیام متنی به چند گیرنده را انجام می‌دهد.
func (h *Handler) SendText(c echo.Context) error {
	var req sendTextRequest
	if err := response.Bind(c, &req); err != nil {
		return err
	}
	if err := req.validate(); err != nil {
		return response.FromError(c, err)
	}

	result, err := h.uc.Send(c.Request().Context(), req.toCommand())
	if err != nil {
		return response.FromError(c, err)
	}
	return response.OK(c, result)
}

// ListMessages لیست پیام‌ها را با فیلتر query برمی‌گرداند.
func (h *Handler) ListMessages(c echo.Context) error {
	var req listMessagesRequest
	if err := c.Bind(&req); err != nil {
		return response.Fail(c, 400, "bad_request", "invalid query parameters")
	}
	if err := req.validate(); err != nil {
		return response.FromError(c, err)
	}

	messages, err := h.uc.ListMessages(c.Request().Context(), req.toFilter())
	if err != nil {
		return response.FromError(c, err)
	}

	return response.OK(c, toMessageListResponse(messages))
}

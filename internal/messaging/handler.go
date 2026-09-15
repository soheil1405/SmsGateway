package messaging

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
	e.POST("/messages/send", h.Send)
	e.GET("/messages", h.ListMessages)
}

func (h *Handler) Send(c echo.Context) error {
	var req sendMessagesRequest
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

	return response.OK(c, toSendResponse(result))
}

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

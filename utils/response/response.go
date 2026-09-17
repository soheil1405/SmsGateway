package response

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/soheil/arvan/utils/errs"
)

// Envelope شکل استاندارد پاسخ API است.
//
// موفقیت: { "data": ... }
// خطا:    { "error": { "code": "...", "message": "...", "fields": [...] } }
type Envelope struct {
	Data  any        `json:"data,omitempty"`
	Error *ErrorBody `json:"error,omitempty"`
}

// ErrorBody بدنهٔ خطای استاندارد است.
type ErrorBody struct {
	Code    string       `json:"code"`
	Message string       `json:"message"`
	Fields  []errs.Field `json:"fields,omitempty"`
}

// OK پاسخ موفق ۲۰۰ برمی‌گرداند.
func OK(c echo.Context, data any) error {
	return c.JSON(http.StatusOK, Envelope{Data: data})
}

// Created پاسخ موفق ۲۰۱ برمی‌گرداند.
func Created(c echo.Context, data any) error {
	return c.JSON(http.StatusCreated, Envelope{Data: data})
}

// NoContent پاسخ ۲۰۴ بدون بدنه برمی‌گرداند.
func NoContent(c echo.Context) error {
	return c.NoContent(http.StatusNoContent)
}

// Fail پاسخ خطا با کد HTTP و کد/پیام کسب‌وکار برمی‌گرداند.
func Fail(c echo.Context, status int, code, message string, fields ...errs.Field) error {
	body := ErrorBody{Code: code, Message: message}
	if len(fields) > 0 {
		body.Fields = fields
	}
	return c.JSON(status, Envelope{Error: &body})
}

// Bind بدنهٔ JSON را به dst می‌بندد؛ در صورت خطا پاسخ ۴۰۰ می‌دهد.
func Bind(c echo.Context, dst any) error {
	if err := c.Bind(dst); err != nil {
		return Fail(c, http.StatusBadRequest, "bad_request", "invalid request body")
	}
	return nil
}

// PathID پارامتر مسیر را به int64 مثبت تبدیل می‌کند.
func PathID(c echo.Context, name string) (int64, error) {
	id, err := strconv.ParseInt(c.Param(name), 10, 64)
	if err != nil || id <= 0 {
		return 0, Fail(c, http.StatusBadRequest, "bad_request", "invalid "+name)
	}
	return id, nil
}

// FromError خطای دامنه/اعتبارسنجی را به پاسخ HTTP مناسب نگاشت می‌کند.
func FromError(c echo.Context, err error) error {
	if err == nil {
		return nil
	}

	if v, ok := errs.AsValidation(err); ok {
		return Fail(c, http.StatusBadRequest, "validation_error", "validation failed", v.Fields...)
	}

	switch {
	case errors.Is(err, errs.ErrNotFound):
		return Fail(c, http.StatusNotFound, "not_found", "resource not found")
	case errors.Is(err, errs.ErrConflict):
		return Fail(c, http.StatusConflict, "conflict", "idempotency key reused with different payload")
	case errors.Is(err, errs.ErrInsufficientBalance):
		return Fail(c, http.StatusPaymentRequired, "insufficient_balance", "insufficient balance")
	default:
		return Fail(c, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

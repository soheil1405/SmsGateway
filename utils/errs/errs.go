package errs

import (
	"errors"
	"fmt"
)

// خطاهای مشترک کسب‌وکار که بین ماژول‌ها استفاده می‌شوند.
var (
	ErrNotFound            = errors.New("not found")
	ErrConflict            = errors.New("conflict")
	ErrInsufficientBalance = errors.New("insufficient balance")
)

// Validation خطای اعتبارسنجی فیلدبه‌فیلد از DTO/handler است.
type Validation struct {
	Fields []Field
}

// Field یک خطای مربوط به یک فیلد ورودی است.
type Field struct {
	Name    string `json:"field"`
	Message string `json:"message"`
}

// Error پیام خلاصهٔ اولین فیلد نامعتبر را برمی‌گرداند.
func (e *Validation) Error() string {
	if len(e.Fields) == 0 {
		return "validation failed"
	}
	return fmt.Sprintf("%s: %s", e.Fields[0].Name, e.Fields[0].Message)
}

// Add یک خطای فیلد به لیست اضافه می‌کند.
func (e *Validation) Add(field, message string) {
	e.Fields = append(e.Fields, Field{Name: field, Message: message})
}

// Ok یعنی هیچ خطای اعتبارسنجی وجود ندارد.
func (e *Validation) Ok() bool {
	return e == nil || len(e.Fields) == 0
}

// Err در صورت وجود خطا، خود Validation را به‌عنوان error برمی‌گرداند.
func (e *Validation) Err() error {
	if e.Ok() {
		return nil
	}
	return e
}

// AsValidation بررسی می‌کند که err از نوع Validation باشد.
func AsValidation(err error) (*Validation, bool) {
	var v *Validation
	ok := errors.As(err, &v)
	return v, ok
}

package errs

import (
	"errors"
	"fmt"
)

// Shared business/sentinel errors used across modules.
var (
	ErrNotFound            = errors.New("not found")
	ErrConflict            = errors.New("conflict")
	ErrInsufficientBalance = errors.New("insufficient balance")
)

// Validation is returned from DTO/handler validation (field-level).
type Validation struct {
	Fields []Field
}

type Field struct {
	Name    string `json:"field"`
	Message string `json:"message"`
}

func (e *Validation) Error() string {
	if len(e.Fields) == 0 {
		return "validation failed"
	}
	return fmt.Sprintf("%s: %s", e.Fields[0].Name, e.Fields[0].Message)
}

func (e *Validation) Add(field, message string) {
	e.Fields = append(e.Fields, Field{Name: field, Message: message})
}

func (e *Validation) Ok() bool {
	return e == nil || len(e.Fields) == 0
}

func (e *Validation) Err() error {
	if e.Ok() {
		return nil
	}
	return e
}

func AsValidation(err error) (*Validation, bool) {
	var v *Validation
	ok := errors.As(err, &v)
	return v, ok
}

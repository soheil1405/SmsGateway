package domain

import "time"

// User موجودیت کاربر با موجودی کیف پول است.
type User struct {
	ID        int64
	Name      string
	Balance   int64 // موجودی به تومان
	CreatedAt time.Time
	UpdatedAt time.Time
}

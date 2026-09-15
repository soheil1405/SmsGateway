package domain

import "time"

type User struct {
	ID        int64
	Name      string
	Balance   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

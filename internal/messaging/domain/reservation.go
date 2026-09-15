package domain

import "time"

type ReservationStatus string

const (
	ReservationPending   ReservationStatus = "pending"
	ReservationCommitted ReservationStatus = "committed"
	ReservationReleased  ReservationStatus = "released"
	ReservationExpired   ReservationStatus = "expired"
)

type BalanceReservation struct {
	ID        int64
	UserID    int64
	RequestID int64
	Amount    int64
	Status    ReservationStatus
	CreatedAt time.Time
	ExpiresAt time.Time
}

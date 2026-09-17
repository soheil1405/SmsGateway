package domain

import "time"

// ReservationStatus وضعیت رزرو موجودی در دیتابیس است.
type ReservationStatus string

const (
	ReservationPending   ReservationStatus = "pending"   // در انتظار commit
	ReservationCommitted ReservationStatus = "committed" // نهایی شده
	ReservationReleased  ReservationStatus = "released"  // آزاد شده
	ReservationExpired   ReservationStatus = "expired"   // منقضی شده
)

// BalanceReservation رزرو مبلغ از موجودی کاربر برای یک request است.
type BalanceReservation struct {
	ID        int64
	UserID    int64
	RequestID int64
	Amount    int64
	Status    ReservationStatus
	CreatedAt time.Time
	ExpiresAt time.Time
}

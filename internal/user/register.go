package user

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/labstack/echo/v4"
)

const defaultSeedUserName = "soheil"

// Register ماژول کاربر را به سرور Echo وصل می‌کند و در صورت نبود، کاربر پیش‌فرض را می‌سازد.
func Register(e *echo.Echo, db *sql.DB) {
	repo := NewRepo(db)
	uc := NewUseCase(repo)
	NewHandler(uc).Register(e)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	u, err := repo.EnsureByName(ctx, defaultSeedUserName)
	if err != nil {
		log.Printf("component=user event=seed_error name=%s err=%v", defaultSeedUserName, err)
		return
	}
	log.Printf("component=user event=seed_ready id=%d name=%s balance=%d", u.ID, u.Name, u.Balance)
}

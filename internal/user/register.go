package user

import (
	"database/sql"

	"github.com/labstack/echo/v4"
)

// Register ماژول کاربر را به سرور Echo وصل می‌کند.
func Register(e *echo.Echo, db *sql.DB) {
	repo := NewRepo(db)
	uc := NewUseCase(repo)
	NewHandler(uc).Register(e)
}

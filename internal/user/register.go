package user

import (
	"database/sql"

	"github.com/labstack/echo/v4"
)

func Register(e *echo.Echo, db *sql.DB) {
	repo := NewRepo(db)
	uc := NewUseCase(repo)
	NewHandler(uc).Register(e)
}

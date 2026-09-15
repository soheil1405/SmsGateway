package messaging

import (
	"database/sql"

	"github.com/labstack/echo/v4"

	"github.com/soheil/arvan/internal/user"
)

func Register(e *echo.Echo, db *sql.DB) {
	repo := NewRepo(db)
	uc := NewUseCase(repo, user.NewRepo(db))
	NewHandler(uc).Register(e)
}

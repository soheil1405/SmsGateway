package main

import (
	"log"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"github.com/soheil/arvan/internal/messaging"
	"github.com/soheil/arvan/internal/user"
	"github.com/soheil/arvan/utils/config"
	"github.com/soheil/arvan/utils/database"
)

func main() {
	cfg := config.Load()

	sqlDB, err := database.Connect(cfg.Database)
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.Recover())
	e.Use(middleware.Logger())

	e.GET("/health", func(c echo.Context) error {
		return c.JSON(200, map[string]string{"status": "ok"})
	})

	user.Register(e, sqlDB)
	messaging.Register(e, sqlDB)

	log.Fatal(e.Start(cfg.Addr))
}

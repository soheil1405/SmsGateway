package main

import (
	"log"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"github.com/soheil/arvan/docs"
	"github.com/soheil/arvan/internal/messaging"
	"github.com/soheil/arvan/internal/user"
	"github.com/soheil/arvan/utils/config"
	"github.com/soheil/arvan/utils/database"
	"github.com/soheil/arvan/utils/kafka"
	"github.com/soheil/arvan/utils/redisx"
)

func main() {
	cfg := config.Load()

	// اتصال به Postgres
	sqlDB, err := database.Connect(cfg.Database)
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	// اتصال به Redis (idempotency و hold موجودی)
	rdb, err := redisx.Connect(cfg.Redis)
	if err != nil {
		log.Fatal(err)
	}
	defer rdb.Close()

	// Producer کافکا برای صف ارسال پیامک
	producer := kafka.NewProducer(cfg.Kafka)
	defer producer.Close()

	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.Recover())
	e.Use(middleware.Logger())

	// بررسی سلامت سرویس
	e.GET("/health", func(c echo.Context) error {
		return c.JSON(200, map[string]string{"status": "ok"})
	})

	docs.Register(e)
	user.Register(e, sqlDB)
	messaging.Register(e, sqlDB, rdb, producer)

	log.Fatal(e.Start(cfg.Addr))
}

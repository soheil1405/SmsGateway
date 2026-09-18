package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"github.com/soheil/arvan/docs"
	"github.com/soheil/arvan/internal/messaging"
	"github.com/soheil/arvan/internal/user"
	"github.com/soheil/arvan/utils/config"
	"github.com/soheil/arvan/utils/database"
	"github.com/soheil/arvan/utils/kafka"
	"github.com/soheil/arvan/utils/metrics"
	"github.com/soheil/arvan/utils/otelx"
	"github.com/soheil/arvan/utils/redisx"
)

func main() {
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	otelShutdown, err := otelx.Setup(ctx, otelx.Config{
		Enabled:  cfg.OTel.Enabled,
		Endpoint: stripOTLPScheme(cfg.OTel.Endpoint),
		Insecure: cfg.OTel.Insecure,
		Service:  cfg.OTel.Service,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := otelShutdown(shutdownCtx); err != nil {
			log.Printf("component=otel event=shutdown_error err=%v", err)
		}
	}()

	sqlDB, err := database.Connect(cfg.Database)
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	rdb, err := redisx.Connect(cfg.Redis)
	if err != nil {
		log.Fatal(err)
	}
	defer rdb.Close()

	producer := kafka.NewProducer(cfg.Kafka)
	defer producer.Close()

	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.Recover())
	e.Use(middleware.Logger())
	e.Use(otelx.EchoMiddleware(cfg.OTel.Service))

	counters := &metrics.Counters{}
	counters.InitOTel()
	e.GET("/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})
	e.GET("/metrics", echo.WrapHandler(http.HandlerFunc(counters.Handler())))

	docs.Register(e)
	user.Register(e, sqlDB)
	rt := messaging.Register(e, sqlDB, rdb, producer, cfg.Kafka, counters)
	rt.Start(ctx)

	go func() {
		<-ctx.Done()
		log.Printf("component=main event=shutdown_start")
		rt.Stop(10 * time.Second)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := e.Shutdown(shutdownCtx); err != nil {
			log.Printf("component=main event=http_shutdown_error err=%v", err)
		}
		log.Printf("component=main event=shutdown_done")
	}()

	log.Printf("component=main event=listen addr=%s", cfg.Addr)
	if err := e.Start(cfg.Addr); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// stripOTLPScheme برای SDK که host:port می‌خواهد، scheme را حذف می‌کند.
func stripOTLPScheme(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	for _, p := range []string{"https://", "http://"} {
		endpoint = strings.TrimPrefix(endpoint, p)
	}
	return endpoint
}

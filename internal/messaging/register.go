package messaging

import (
	"context"
	"database/sql"
	"log"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"

	"github.com/soheil/arvan/utils/config"
	"github.com/soheil/arvan/utils/kafka"
	"github.com/soheil/arvan/utils/metrics"
)

// Runtime پردازش‌های پس‌زمینهٔ ماژول messaging را نگه می‌دارد.
type Runtime struct {
	Outbox   *OutboxWorker
	Consumer *SMSConsumer
	Metrics  *metrics.Counters

	wg sync.WaitGroup
}

// Start outbox worker و مصرف‌کنندهٔ پیامک را در پس‌زمینه اجرا می‌کند.
func (r *Runtime) Start(ctx context.Context) {
	r.wg.Add(2)
	go func() {
		defer r.wg.Done()
		r.Outbox.Run(ctx)
	}()
	go func() {
		defer r.wg.Done()
		r.Consumer.Run(ctx)
	}()
}

// Wait تا پایان workerها صبر می‌کند (بعد از cancel شدن context).
func (r *Runtime) Wait() {
	r.wg.Wait()
}

// Stop با timeout منتظر اتمام workerها می‌ماند.
func (r *Runtime) Stop(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		log.Printf("component=runtime event=stopped")
	case <-time.After(timeout):
		log.Printf("component=runtime event=stop_timeout timeout=%s", timeout)
	}
}

// Register ماژول messaging را وصل می‌کند و پردازش‌های پس‌زمینه را برمی‌گرداند.
func Register(
	e *echo.Echo,
	db *sql.DB,
	rdb *redis.Client,
	producer *kafka.Producer,
	cfg config.Kafka,
	m *metrics.Counters,
) *Runtime {
	if m == nil {
		m = &metrics.Counters{}
	}
	repo := NewRepo(db)
	uc := NewUseCase(repo, NewRedisStore(rdb), cfg.TopicNormal, cfg.TopicExpress, m)
	NewHandler(uc).Register(e)

	newReader := func(topic, groupID string) LaneReader {
		return kafka.NewConsumer(cfg.Brokers, topic, groupID)
	}

	expressWorkers := capLaneWorkers(cfg.Brokers, cfg.TopicExpress, cfg.WorkersExpress)
	normalWorkers := capLaneWorkers(cfg.Brokers, cfg.TopicNormal, cfg.WorkersNormal)

	return &Runtime{
		Metrics: m,
		Outbox: NewOutboxWorker(
			repo, producer, producer,
			500*time.Millisecond, 50, cfg.OutboxMaxAttempts, m,
		),
		Consumer: NewSMSConsumer(
			NewMessageProcessor(repo, &LogSender{}, producer, m),
			newReader,
			Lane{Name: "express", Topic: cfg.TopicExpress, GroupID: cfg.GroupExpress, Workers: expressWorkers},
			Lane{Name: "normal", Topic: cfg.TopicNormal, GroupID: cfg.GroupNormal, Workers: normalWorkers},
		),
	}
}

func capLaneWorkers(brokers []string, topic string, requested int) int {
	n, err := kafka.PartitionCount(brokers, topic)
	if err != nil {
		log.Printf("component=runtime event=partition_meta topic=%s err=%v; using requested=%d", topic, err, requested)
		return kafka.CapWorkers(requested, 0)
	}
	capped := kafka.CapWorkers(requested, n)
	if capped != requested {
		log.Printf("component=runtime event=workers_capped topic=%s requested=%d partitions=%d workers=%d",
			topic, requested, n, capped)
	}
	return capped
}

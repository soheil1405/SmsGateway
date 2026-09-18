package messaging

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/soheil/arvan/internal/messaging/domain"
	"github.com/soheil/arvan/utils/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

const outboxClaimStaleAfter = time.Minute

// MessagePublisher قرارداد انتشار به Kafka برای OutboxWorker است.
type MessagePublisher interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
}

// OutboxWorker رویدادهای pending را claim می‌کند و به Kafka می‌فرستد.
type OutboxWorker struct {
	repo        *Repo
	publisher   MessagePublisher
	dlq         DLQPublisher
	interval    time.Duration
	batchSize   int
	maxAttempts int
	metrics     *metrics.Counters
}

// NewOutboxWorker یک worker با سقف تلاش و DLQ می‌سازد.
func NewOutboxWorker(
	repo *Repo,
	publisher MessagePublisher,
	dlq DLQPublisher,
	interval time.Duration,
	batchSize, maxAttempts int,
	m *metrics.Counters,
) *OutboxWorker {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	if batchSize <= 0 {
		batchSize = 50
	}
	if maxAttempts <= 0 {
		maxAttempts = 10
	}
	if m == nil {
		m = &metrics.Counters{}
	}
	return &OutboxWorker{
		repo:        repo,
		publisher:   publisher,
		dlq:         dlq,
		interval:    interval,
		batchSize:   batchSize,
		maxAttempts: maxAttempts,
		metrics:     m,
	}
}

// Run تا لغو context، به‌صورت دوره‌ای ProcessOnce را اجرا می‌کند.
func (w *OutboxWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.ProcessOnce(ctx); err != nil {
				log.Printf("component=outbox-worker event=error err=%v", err)
			}
		}
	}
}

// ProcessOnce یک batch را claim، publish و در صورت موفقیت mark می‌کند.
// بعد از maxAttempts، event به failed می‌رود و به DLQ منتقل می‌شود.
func (w *OutboxWorker) ProcessOnce(ctx context.Context) error {
	ctx, span := otel.Tracer("arvan/messaging").Start(ctx, "outbox.ProcessOnce")
	defer span.End()

	events, err := w.repo.ClaimPendingOutbox(ctx, w.batchSize, outboxClaimStaleAfter)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if len(events) == 0 {
		return nil
	}

	w.metrics.IncOutboxClaimed(ctx, int64(len(events)))
	span.SetAttributes(attribute.Int("outbox.claimed", len(events)))
	log.Printf("component=outbox-worker event=claimed count=%d", len(events))

	for i := range events {
		e := &events[i]
		if e.Attempts > w.maxAttempts {
			w.deadLetter(ctx, e, "max_attempts_exceeded")
			continue
		}

		if err := w.publisher.Publish(ctx, e.Topic, e.PartitionKey, e.Payload); err != nil {
			log.Printf("component=outbox-worker event=publish_failed outbox_id=%d attempts=%d err=%v",
				e.ID, e.Attempts, err)
			w.metrics.IncOutboxFailed(ctx, 1)
			if e.Attempts >= w.maxAttempts {
				w.deadLetter(ctx, e, err.Error())
				continue
			}
			_ = w.repo.RecordOutboxFailure(ctx, e.ID, err.Error())
			continue
		}
		if err := w.repo.MarkOutboxPublished(ctx, e.ID); err != nil {
			log.Printf("component=outbox-worker event=mark_failed outbox_id=%d err=%v", e.ID, err)
			continue
		}
		w.metrics.IncOutboxPublished(ctx, 1)
		log.Printf("component=outbox-worker event=published outbox_id=%d aggregate_id=%d topic=%s",
			e.ID, e.AggregateID, e.Topic)
	}
	return nil
}

func (w *OutboxWorker) deadLetter(ctx context.Context, e *domain.OutboxEvent, detail string) {
	body, err := json.Marshal(map[string]any{
		"reason":      "outbox_exhausted",
		"detail":      detail,
		"outboxId":    e.ID,
		"aggregateId": e.AggregateID,
		"topic":       e.Topic,
		"attempts":    e.Attempts,
		"payload":     e.Payload,
		"at":          time.Now().UTC().Format(time.RFC3339),
	})
	if err == nil && w.dlq != nil {
		key := strconv.FormatInt(e.AggregateID, 10)
		if pubErr := w.dlq.PublishDLQ(ctx, key, body); pubErr != nil {
			log.Printf("component=outbox-worker event=dlq_error outbox_id=%d err=%v", e.ID, pubErr)
		} else {
			w.metrics.IncOutboxToDLQ(ctx, 1)
		}
	}
	if err := w.repo.MarkOutboxFailed(ctx, e.ID, detail); err != nil {
		log.Printf("component=outbox-worker event=mark_failed_status outbox_id=%d err=%v", e.ID, err)
		_ = w.repo.RecordOutboxFailure(ctx, e.ID, detail)
		return
	}
	log.Printf("component=outbox-worker event=dead_letter outbox_id=%d attempts=%d", e.ID, e.Attempts)
}

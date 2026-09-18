package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/soheil/arvan/utils/kafka"
	"github.com/soheil/arvan/utils/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	sendingStaleAfter   = 2 * time.Minute
	maxProviderAttempts = 3
	providerRetryBase   = 100 * time.Millisecond
)

// errRetryLater یعنی offset را commit نکن؛ پیام دوباره تحویل شود.
var errRetryLater = errors.New("retry later")

// SMSSender ارسال به provider است.
// idempotencyKey باید برای همان messageId ثابت باشد تا provider در صورت
// تحویل تکراری Kafka، SMS را دوباره نفرستد.
type SMSSender interface {
	Send(ctx context.Context, idempotencyKey, recipient, text string) error
}

// LogSender provider ساختگی با idempotency درون‌حافظه‌ای برای محیط توسعه است.
// محدودیت: state فقط داخل یک process است و بعد از restart از بین می‌رود.
// Provider واقعی باید کلید idempotency را سمت gateway پشتیبانی کند.
type LogSender struct {
	seen sync.Map // key → struct{}
}

func (s *LogSender) Send(_ context.Context, idempotencyKey, recipient, text string) error {
	if idempotencyKey == "" {
		idempotencyKey = recipient + ":" + text
	}
	if _, loaded := s.seen.LoadOrStore(idempotencyKey, struct{}{}); loaded {
		log.Printf("component=sms-provider event=idempotent_skip key=%s recipient=%s", idempotencyKey, recipient)
		return nil
	}
	log.Printf("component=sms-provider event=sent key=%s recipient=%s text=%q", idempotencyKey, recipient, text)
	return nil
}

// DLQPublisher پیام‌های سمی / شکست‌خوردهٔ نهایی را به تاپیک DLQ می‌فرستد.
type DLQPublisher interface {
	PublishDLQ(ctx context.Context, key string, value []byte) error
}

// smsEvent همان payload تولیدشده در outbox است.
type smsEvent struct {
	MessageID    int64  `json:"messageId"`
	RequestID    int64  `json:"requestId"`
	UserID       int64  `json:"userId"`
	Recipient    string `json:"recipient"`
	Text         string `json:"text"`
	Type         string `json:"type"`
	DeliveryMode string `json:"deliveryMode"`
	Cost         int64  `json:"cost"`
}

// PayloadProcessor یک رویداد Kafka را پردازش می‌کند.
type PayloadProcessor interface {
	Process(ctx context.Context, payload []byte) error
}

// MessageProcessor پیام را ارسال و وضعیتش را در Postgres به‌روز می‌کند.
type MessageProcessor struct {
	repo   *Repo
	sender SMSSender
	dlq    DLQPublisher
	metrics *metrics.Counters
}

// NewMessageProcessor یک پردازشگر با provider و DLQ اختیاری می‌سازد.
func NewMessageProcessor(repo *Repo, sender SMSSender, dlq DLQPublisher, m *metrics.Counters) *MessageProcessor {
	if m == nil {
		m = &metrics.Counters{}
	}
	return &MessageProcessor{repo: repo, sender: sender, dlq: dlq, metrics: m}
}

// Process رویداد را idempotent پردازش می‌کند.
// nil → commit offset؛ errRetryLater / خطای دیگر → بدون commit.
func (p *MessageProcessor) Process(ctx context.Context, payload []byte) error {
	ctx, span := otel.Tracer("arvan/messaging").Start(ctx, "sms.Process",
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer span.End()

	var event smsEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		log.Printf("component=sms-consumer event=invalid_payload err=%v", err)
		p.toDLQ(ctx, "invalid_payload", "0", payload, err.Error())
		span.SetAttributes(attribute.String("outcome", "invalid_payload"))
		return nil
	}
	if event.MessageID == 0 {
		log.Printf("component=sms-consumer event=missing_message_id")
		p.toDLQ(ctx, "missing_message_id", "0", payload, "messageId required")
		span.SetAttributes(attribute.String("outcome", "missing_message_id"))
		return nil
	}

	key := providerIdempotencyKey(event.MessageID)
	span.SetAttributes(
		attribute.Int64("message.id", event.MessageID),
		attribute.String("delivery.mode", event.DeliveryMode),
		attribute.String("recipient", event.Recipient),
	)

	outcome, err := p.repo.ClaimMessageForSending(ctx, event.MessageID, sendingStaleAfter)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	switch outcome {
	case ClaimAlreadySent, ClaimAlreadyFailed, ClaimNotFound:
		log.Printf("component=sms-consumer event=skip message_id=%d outcome=%d", event.MessageID, outcome)
		span.SetAttributes(attribute.String("outcome", "skip"))
		return nil
	case ClaimInFlight:
		p.metrics.IncSMSInFlightSkip(ctx, 1)
		log.Printf("component=sms-consumer event=in_flight message_id=%d", event.MessageID)
		span.SetAttributes(attribute.String("outcome", "in_flight"))
		return errRetryLater
	case ClaimAcquired:
		// ادامه
	}

	if err := p.sendWithRetry(ctx, event); err != nil {
		log.Printf("component=sms-consumer event=provider_failed message_id=%d err=%v", event.MessageID, err)
		if markErr := p.repo.MarkMessageFailed(ctx, event.MessageID, "provider_error"); markErr != nil {
			span.RecordError(markErr)
			return markErr
		}
		p.metrics.IncSMSFailed(ctx, 1)
		p.toDLQ(ctx, "provider_error", key, payload, err.Error())
		span.SetAttributes(attribute.String("outcome", "failed"))
		return nil
	}

	if err := p.repo.MarkMessageSent(ctx, event.MessageID); err != nil {
		log.Printf("component=sms-consumer event=mark_sent_failed message_id=%d err=%v", event.MessageID, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	p.metrics.IncSMSSent(ctx, 1)
	span.SetAttributes(attribute.String("outcome", "sent"))
	log.Printf("component=sms-consumer event=sent message_id=%d recipient=%s mode=%s",
		event.MessageID, event.Recipient, event.DeliveryMode)
	return nil
}

func (p *MessageProcessor) sendWithRetry(ctx context.Context, event smsEvent) error {
	idemKey := providerIdempotencyKey(event.MessageID)
	var last error
	for attempt := 1; attempt <= maxProviderAttempts; attempt++ {
		last = p.sender.Send(ctx, idemKey, event.Recipient, event.Text)
		if last == nil {
			return nil
		}
		if attempt == maxProviderAttempts {
			break
		}
		p.metrics.IncSMSProviderRetry(ctx, 1)
		delay := providerRetryBase * time.Duration(1<<(attempt-1))
		delay += time.Duration(rand.Intn(50)) * time.Millisecond
		log.Printf("component=sms-consumer event=provider_retry message_id=%d attempt=%d delay=%s err=%v",
			event.MessageID, attempt, delay, last)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return last
}

func (p *MessageProcessor) toDLQ(ctx context.Context, reason, key string, payload []byte, detail string) {
	if p.dlq == nil {
		return
	}
	body, err := json.Marshal(map[string]any{
		"reason":       reason,
		"detail":       detail,
		"payload":      string(payload),
		"payloadBytes": len(payload),
		"at":           time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		log.Printf("component=sms-consumer event=dlq_marshal_error err=%v", err)
		return
	}
	if err := p.dlq.PublishDLQ(ctx, key, body); err != nil {
		log.Printf("component=sms-consumer event=dlq_publish_error err=%v", err)
		return
	}
	p.metrics.IncConsumerToDLQ(ctx, 1)
	log.Printf("component=sms-consumer event=dlq reason=%s key=%s", reason, key)
}

// LaneReader حداقل قرارداد لازم برای خواندن از یک تاپیک است.
type LaneReader interface {
	Fetch(ctx context.Context) (kafka.Record, error)
	Commit(ctx context.Context, rec kafka.Record) error
	Close() error
}

// ReaderFactory برای هر worker یک reader مستقل در همان consumer group می‌سازد.
type ReaderFactory func(topic, groupID string) LaneReader

// Lane یک کلاس ارسال (express یا normal) با ظرفیت اختصاصی خودش است.
type Lane struct {
	Name    string
	Topic   string
	GroupID string
	Workers int
}

// SMSConsumer پیام‌ها را از دو lane مستقل مصرف می‌کند.
type SMSConsumer struct {
	processor PayloadProcessor
	newReader ReaderFactory
	lanes     []Lane
}

// NewSMSConsumer دو lane express/normal را با ظرفیت نامتقارن می‌سازد.
func NewSMSConsumer(
	processor PayloadProcessor,
	newReader ReaderFactory,
	expressLane, normalLane Lane,
) *SMSConsumer {
	if expressLane.Workers < 1 {
		expressLane.Workers = 1
	}
	if normalLane.Workers < 1 {
		normalLane.Workers = 1
	}
	return &SMSConsumer{
		processor: processor,
		newReader: newReader,
		lanes:     []Lane{expressLane, normalLane},
	}
}

// Run تا لغو context، همهٔ workerهای هر دو lane را اجرا می‌کند.
func (c *SMSConsumer) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, lane := range c.lanes {
		for i := 0; i < lane.Workers; i++ {
			wg.Add(1)
			go func(lane Lane, idx int) {
				defer wg.Done()
				c.runWorker(ctx, lane, idx)
			}(lane, i)
		}
	}
	wg.Wait()
}

func (c *SMSConsumer) runWorker(ctx context.Context, lane Lane, idx int) {
	reader := c.newReader(lane.Topic, lane.GroupID)
	defer reader.Close()

	var failStreak int
	for {
		rec, err := reader.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			failStreak++
			backoffSleep(ctx, failStreak)
			log.Printf("component=sms-consumer event=fetch_error lane=%s worker=%d err=%v", lane.Name, idx, err)
			continue
		}

		if err := c.processor.Process(ctx, rec.Value); err != nil {
			failStreak++
			log.Printf("component=sms-consumer event=process_error lane=%s worker=%d streak=%d err=%v",
				lane.Name, idx, failStreak, err)
			backoffSleep(ctx, failStreak)
			continue
		}

		failStreak = 0
		if err := reader.Commit(ctx, rec); err != nil {
			log.Printf("component=sms-consumer event=commit_error lane=%s worker=%d err=%v", lane.Name, idx, err)
		}
	}
}

func backoffSleep(ctx context.Context, streak int) {
	if streak < 1 {
		streak = 1
	}
	if streak > 6 {
		streak = 6
	}
	delay := time.Duration(1<<(streak-1)) * 50 * time.Millisecond
	delay += time.Duration(rand.Intn(50)) * time.Millisecond
	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
}

// providerIdempotencyKey کلید پایدار برای provider است (فعلاً messageId).
func providerIdempotencyKey(messageID int64) string {
	return fmt.Sprintf("msg:%d", messageID)
}

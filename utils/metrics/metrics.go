package metrics

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// Counters شمارنده‌های in-process + (اختیاری) متریک OTel.
type Counters struct {
	IdempotentHits   atomic.Int64 `json:"-"`
	SendAccepted     atomic.Int64 `json:"-"`
	OutboxClaimed    atomic.Int64 `json:"-"`
	OutboxPublished  atomic.Int64 `json:"-"`
	OutboxFailed     atomic.Int64 `json:"-"`
	OutboxToDLQ      atomic.Int64 `json:"-"`
	SMSSent          atomic.Int64 `json:"-"`
	SMSFailed        atomic.Int64 `json:"-"`
	SMSProviderRetry atomic.Int64 `json:"-"`
	SMSInFlightSkip  atomic.Int64 `json:"-"`
	ConsumerToDLQ    atomic.Int64 `json:"-"`

	otelIdempotentHits   metric.Int64Counter
	otelSendAccepted     metric.Int64Counter
	otelOutboxClaimed    metric.Int64Counter
	otelOutboxPublished  metric.Int64Counter
	otelOutboxFailed     metric.Int64Counter
	otelOutboxToDLQ      metric.Int64Counter
	otelSMSSent          metric.Int64Counter
	otelSMSFailed        metric.Int64Counter
	otelSMSProviderRetry metric.Int64Counter
	otelSMSInFlightSkip  metric.Int64Counter
	otelConsumerToDLQ    metric.Int64Counter
}

// InitOTel ابزارهای متریک را به MeterProvider سراسری وصل می‌کند (بعد از otelx.Setup).
func (c *Counters) InitOTel() {
	m := otel.Meter("arvan/messaging")
	must := func(name, desc string) metric.Int64Counter {
		ctr, err := m.Int64Counter(name, metric.WithDescription(desc))
		if err != nil {
			log.Printf("component=metrics event=otel_counter_error name=%s err=%v", name, err)
			return nil
		}
		return ctr
	}
	c.otelIdempotentHits = must("messaging.idempotent_hits", "Idempotent send cache/DB hits")
	c.otelSendAccepted = must("messaging.send_accepted", "Successful new send transactions")
	c.otelOutboxClaimed = must("messaging.outbox_claimed", "Outbox events claimed")
	c.otelOutboxPublished = must("messaging.outbox_published", "Outbox events published to Kafka")
	c.otelOutboxFailed = must("messaging.outbox_failed", "Outbox publish failures")
	c.otelOutboxToDLQ = must("messaging.outbox_to_dlq", "Outbox events sent to DLQ")
	c.otelSMSSent = must("messaging.sms_sent", "SMS marked sent")
	c.otelSMSFailed = must("messaging.sms_failed", "SMS marked failed")
	c.otelSMSProviderRetry = must("messaging.sms_provider_retry", "Provider send retries")
	c.otelSMSInFlightSkip = must("messaging.sms_inflight_skip", "In-flight claim skips")
	c.otelConsumerToDLQ = must("messaging.consumer_to_dlq", "Consumer payloads sent to DLQ")
}

func add(ctx context.Context, ctr metric.Int64Counter, n int64) {
	if ctr != nil && n != 0 {
		ctr.Add(ctx, n)
	}
}

// IncIdempotentHits هم local و هم OTel را افزایش می‌دهد.
func (c *Counters) IncIdempotentHits(ctx context.Context, n int64) {
	c.IdempotentHits.Add(n)
	add(ctx, c.otelIdempotentHits, n)
}

func (c *Counters) IncSendAccepted(ctx context.Context, n int64) {
	c.SendAccepted.Add(n)
	add(ctx, c.otelSendAccepted, n)
}

func (c *Counters) IncOutboxClaimed(ctx context.Context, n int64) {
	c.OutboxClaimed.Add(n)
	add(ctx, c.otelOutboxClaimed, n)
}

func (c *Counters) IncOutboxPublished(ctx context.Context, n int64) {
	c.OutboxPublished.Add(n)
	add(ctx, c.otelOutboxPublished, n)
}

func (c *Counters) IncOutboxFailed(ctx context.Context, n int64) {
	c.OutboxFailed.Add(n)
	add(ctx, c.otelOutboxFailed, n)
}

func (c *Counters) IncOutboxToDLQ(ctx context.Context, n int64) {
	c.OutboxToDLQ.Add(n)
	add(ctx, c.otelOutboxToDLQ, n)
}

func (c *Counters) IncSMSSent(ctx context.Context, n int64) {
	c.SMSSent.Add(n)
	add(ctx, c.otelSMSSent, n)
}

func (c *Counters) IncSMSFailed(ctx context.Context, n int64) {
	c.SMSFailed.Add(n)
	add(ctx, c.otelSMSFailed, n)
}

func (c *Counters) IncSMSProviderRetry(ctx context.Context, n int64) {
	c.SMSProviderRetry.Add(n)
	add(ctx, c.otelSMSProviderRetry, n)
}

func (c *Counters) IncSMSInFlightSkip(ctx context.Context, n int64) {
	c.SMSInFlightSkip.Add(n)
	add(ctx, c.otelSMSInFlightSkip, n)
}

func (c *Counters) IncConsumerToDLQ(ctx context.Context, n int64) {
	c.ConsumerToDLQ.Add(n)
	add(ctx, c.otelConsumerToDLQ, n)
}

// Snapshot مقادیر فعلی را برای پاسخ HTTP برمی‌گرداند.
func (c *Counters) Snapshot() map[string]int64 {
	return map[string]int64{
		"idempotent_hits":    c.IdempotentHits.Load(),
		"send_accepted":      c.SendAccepted.Load(),
		"outbox_claimed":     c.OutboxClaimed.Load(),
		"outbox_published":   c.OutboxPublished.Load(),
		"outbox_failed":      c.OutboxFailed.Load(),
		"outbox_to_dlq":      c.OutboxToDLQ.Load(),
		"sms_sent":           c.SMSSent.Load(),
		"sms_failed":         c.SMSFailed.Load(),
		"sms_provider_retry": c.SMSProviderRetry.Load(),
		"sms_inflight_skip":  c.SMSInFlightSkip.Load(),
		"consumer_to_dlq":    c.ConsumerToDLQ.Load(),
	}
}

// Handler یک endpoint JSON روی /metrics ثبت می‌کند.
func (c *Counters) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"metrics": c.Snapshot(),
		})
	}
}

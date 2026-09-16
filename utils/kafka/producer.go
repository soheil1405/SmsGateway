package kafka

import (
	"context"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/soheil/arvan/utils/config"
)

// Producer نویسندهٔ پیام به تاپیک‌های Kafka است.
type Producer struct {
	writer *kafkago.Writer
	topics config.Kafka
}

// NewProducer یک Writer با تنظیمات پیش‌فرض پروژه می‌سازد.
func NewProducer(cfg config.Kafka) *Producer {
	return &Producer{
		topics: cfg,
		writer: &kafkago.Writer{
			Addr:                   kafkago.TCP(cfg.Brokers...),
			Balancer:               &kafkago.Hash{},
			RequiredAcks:           kafkago.RequireOne,
			AllowAutoTopicCreation: true, // برای محیط توسعه تاپیک را خودکار بسازد
			Async:                  false,
		},
	}
}

// Publish یک پیام تکی را منتشر می‌کند.
func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte) error {
	return p.PublishBatch(ctx, []Message{{Topic: topic, Key: key, Value: value}})
}

// Message یک پیام آمادهٔ انتشار است.
type Message struct {
	Topic string
	Key   string
	Value []byte
}

// PublishBatch همهٔ پیام‌ها را در یک WriteMessages می‌فرستد.
func (p *Producer) PublishBatch(ctx context.Context, batch []Message) error {
	if len(batch) == 0 {
		return nil
	}
	msgs := make([]kafkago.Message, 0, len(batch))
	now := time.Now()
	for _, m := range batch {
		msgs = append(msgs, kafkago.Message{
			Topic: m.Topic,
			Key:   []byte(m.Key),
			Value: m.Value,
			Time:  now,
		})
	}
	if err := p.writer.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("publish kafka batch size=%d: %w", len(batch), err)
	}
	return nil
}

// Close Writer را می‌بندد و منابع را آزاد می‌کند.
func (p *Producer) Close() error {
	return p.writer.Close()
}

// TopicNormal نام تاپیک ارسال عادی را برمی‌گرداند.
func (p *Producer) TopicNormal() string { return p.topics.TopicNormal }

// TopicExpress نام تاپیک ارسال فوری را برمی‌گرداند.
func (p *Producer) TopicExpress() string { return p.topics.TopicExpress }

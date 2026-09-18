package kafka

import (
	"context"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Record یک رکورد خوانده‌شده از Kafka است.
// raw برای commit همان offset نگه داشته می‌شود.
type Record struct {
	Topic     string
	Partition int
	Offset    int64
	Key       string
	Value     []byte

	raw kafkago.Message
}

// Consumer یک عضو consumer group روی یک تاپیک است.
type Consumer struct {
	reader *kafkago.Reader
}

// NewConsumer یک reader با commit دستی می‌سازد.
// MaxPollRecords معادل ندارد؛ FetchMessage تک‌تک می‌خواند تا نقاط تصمیم مکرر باشند.
func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:        brokers,
			Topic:          topic,
			GroupID:        groupID,
			MinBytes:       1,
			MaxBytes:       10 << 20,
			MaxWait:        200 * time.Millisecond,
			CommitInterval: 0, // commit فقط با فراخوانی صریح
		}),
	}
}

// Fetch یک پیام می‌خواند بدون اینکه offset را commit کند.
func (c *Consumer) Fetch(ctx context.Context) (Record, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return Record{}, err
	}
	return Record{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       string(msg.Key),
		Value:     msg.Value,
		raw:       msg,
	}, nil
}

// Commit فقط بعد از پردازش موفق صدا زده می‌شود (at-least-once).
func (c *Consumer) Commit(ctx context.Context, rec Record) error {
	return c.reader.CommitMessages(ctx, rec.raw)
}

// Close اتصال consumer را می‌بندد.
func (c *Consumer) Close() error {
	return c.reader.Close()
}

// PartitionCount تعداد پارتیشن یک تاپیک را برمی‌گرداند.
// اگر metadata در دسترس نباشد، 0 و خطا برمی‌گرداند.
func PartitionCount(brokers []string, topic string) (int, error) {
	if len(brokers) == 0 {
		return 0, fmt.Errorf("no brokers")
	}
	conn, err := kafkago.Dial("tcp", brokers[0])
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	parts, err := conn.ReadPartitions(topic)
	if err != nil {
		return 0, err
	}
	seen := make(map[int]struct{}, len(parts))
	for _, p := range parts {
		if p.Topic == topic {
			seen[p.ID] = struct{}{}
		}
	}
	return len(seen), nil
}

// CapWorkers تعداد worker را به سقف پارتیشن محدود می‌کند.
func CapWorkers(requested, partitions int) int {
	if requested < 1 {
		requested = 1
	}
	if partitions > 0 && requested > partitions {
		return partitions
	}
	return requested
}

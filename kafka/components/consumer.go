package components

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
)

type ConsumerConfig struct {
	GroupID     string
	MinBytes    int
	MaxBytes    int
	StartOffset int64
	Logger      kafka.Logger
}

type Consumer struct {
	reader *kafka.Reader
}

func (Consumer) New(topic string, brokerAddress string, config ConsumerConfig) (c *Consumer) {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{brokerAddress},
			GroupID:     config.GroupID,
			Topic:       topic,
			MinBytes:    config.MinBytes,
			MaxBytes:    config.MaxBytes,
			StartOffset: config.StartOffset,
			Logger:      config.Logger,
		}),
	}
}

func (c *Consumer) Consume(ctx context.Context) {
	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			break
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		if err := c.reader.CommitMessages(ctx, m); err != nil {
			log.Fatal("failed to commit messages:", err)
		}
	}
}
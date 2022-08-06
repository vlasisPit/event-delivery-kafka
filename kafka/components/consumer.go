package components

import (
	"context"
	"event-delivery-kafka/kafka/backoff"
	"event-delivery-kafka/kafka/processors"
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
	reader                        *kafka.Reader
	processor                     processors.Processor
	exponentialBackOffWithRetries backoff.ExponentialBackOffWithRetries
}

func (Consumer) New(topic string, brokerAddress string, config ConsumerConfig, processor *processors.Processor, backoffStrategy backoff.ExponentialBackOffWithRetries) *Consumer {
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
		processor:                     *processor,
		exponentialBackOffWithRetries: backoffStrategy,
	}
}

func (c *Consumer) Consume(ctx context.Context) {
	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			break
		}

		operation := func() error {
			return c.processor.Action(m)
		}

		if err := c.exponentialBackOffWithRetries.Run(operation); err != nil {
			log.Printf("failed to run operation using exponential backoff strategy: %v for key %s \n", err.Error(), string(m.Key))
		}

		if err := c.reader.CommitMessages(ctx, m); err != nil {
			log.Printf("failed to commit messages: %v for key %s \n", err.Error(), string(m.Key))
		}
	}
}

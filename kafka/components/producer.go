package components

import (
	"context"
	"event-delivery-kafka/models"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

type KafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type ProducerConfig struct {
	Balancer     kafka.Balancer
	WriteTimeout time.Duration
	ReadTimeout  time.Duration
	RequiredAcks kafka.RequiredAcks
	Logger       kafka.Logger
}

type Producer struct {
	Writer KafkaWriter
}

func (Producer) New(topic string, brokerAddress string, config ProducerConfig) *Producer {
	return &Producer{
		Writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokerAddress),
			Topic:                  topic,
			Balancer:               config.Balancer,
			WriteTimeout:           config.WriteTimeout,
			ReadTimeout:            config.ReadTimeout,
			RequiredAcks:           config.RequiredAcks,
			AllowAutoTopicCreation: true,
		},
	}
}

func (producer *Producer) Send(ctx context.Context, msgs ...models.KafkaMessage) error {
	messages := make([]kafka.Message, len(msgs))
	for i := range msgs {
		messages[i] = kafka.Message{
			Key:   []byte(msgs[i].Key),
			Value: []byte(msgs[i].Value),
			Time:  msgs[i].Timestamp,
		}
	}

	return producer.Writer.WriteMessages(ctx, messages...)
}

func (producer *Producer) Close() error {
	if err := producer.Writer.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
		return err
	}
	return nil
}

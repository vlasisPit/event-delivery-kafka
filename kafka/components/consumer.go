package components

import (
	"context"
	"event-delivery-kafka/kafka/backoff"
	"event-delivery-kafka/kafka/processors"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
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
	c := &Consumer{
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

	c.Close(config.GroupID)
	return c
}

/*
According to documentation
Note that it is important to call Close() on a Reader when a process exits. The kafka server needs a graceful
disconnect to stop it from continuing to attempt to send messages to the connected clients. The given example will
not call Close() if the process is terminated with SIGINT (ctrl-c at the shell) or SIGTERM (as docker stop or a
kubernetes restart does). This can result in a delay when a new reader on the same topic connects (e.g. new process
started or new container running). Use a signal.Notify handler to close the reader on process shutdown.
 */
func (c *Consumer) Close(groupId string) {
	signalHandler := make(chan os.Signal, 1)
	signal.Notify(signalHandler, os.Interrupt)
	signal.Notify(signalHandler, syscall.SIGTERM)
	go func() {
		<-signalHandler
		if err := c.reader.Close(); err != nil {
			log.Fatalf("failed to close reader with groupdId %s with error %s \n", groupId, err.Error())
		}
		log.Printf("Consumer reader for groupId %s closed \n", groupId)
		time.Sleep(1 * time.Second)
		os.Exit(1)
	}()
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

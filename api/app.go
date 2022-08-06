package api

import (
	"context"
	"event-delivery-kafka/api/server"
	"event-delivery-kafka/delivery/destinations/mocks"
	backoffStr "event-delivery-kafka/kafka/backoff"
	"event-delivery-kafka/kafka/components"
	"event-delivery-kafka/kafka/processors"
	"event-delivery-kafka/models"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/segmentio/kafka-go"
	"log"
	"net/http"
	"os"
	"time"
)

type App struct {
	Port          string
	Topic         string
	BrokerAddress string
}

func (a *App) Run() {
	a.checkIfTopicExistsAndCreate(a.BrokerAddress)
	a.createAndStartConsumers()

	mux := http.NewServeMux()
	s := server.Server{
		Mux:      mux,
		Producer: a.createProducer(),
	}
	s.Initialize(a.Port)
}

func (a *App) createProducer() *components.Producer {
	producerConfig := components.ProducerConfig{
		Balancer:     &kafka.Murmur2Balancer{}, //ensures that messages with the same key are routed to the same partition
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
		RequiredAcks: kafka.RequireOne, // wait for the leader to acknowledge the writes
		Logger:       log.New(os.Stdout, "kafka writer: ", 0),
	}

	return components.Producer{}.New(a.Topic, a.BrokerAddress, producerConfig)
}

func (a *App) createAndStartConsumers() {
	destinations := [3]mocks.Destination{
		mocks.BigqueryMock{}.New(),
		mocks.PostgresMock{}.New(),
		mocks.SnowflakeMock{}.New(),
	}

	for i, _ := range destinations {
		consumerConfig := components.ConsumerConfig{
			GroupID:     "event-delivery-kafka-" + destinations[i].Name(), //different group Id for each consumer
			MinBytes:    10e3,                                  // 10KB
			MaxBytes:    10e6,                                  // 10MB
			StartOffset: kafka.FirstOffset,
			Logger:      log.New(os.Stdout, fmt.Sprintf("kafka reader for groupId : event-delivery-kafka-%s", destinations[i].Name()), 0),
		}

		backoffStrategy := a.createBackOffStrategy()

		consumer := components.Consumer{}.New(a.Topic, a.BrokerAddress, consumerConfig, processors.Processor{}.New(a.createConsumerAction(destinations[i])), *backoffStrategy)
		go consumer.Consume(context.Background())
	}
}

func (a *App) createConsumerAction(dest mocks.Destination) func(message kafka.Message) error {
	return func(message kafka.Message) error {
		ev := models.Event{}.New(string(message.Key), string(message.Value))
		if err := dest.Receive(*ev); err != nil {
			log.Printf("failed to send message: %v for key %s \n", err.Error(), string(message.Key))
			return err
		}
		return nil
	}
}

func (a *App) checkIfTopicExistsAndCreate(brokerAddress string) {
	config := []kafka.ConfigEntry{{
		ConfigName:  "log.retention.hours",
		ConfigValue: "24",
	}}

	topicConfig := components.TopicConfig{
		Topic:             a.Topic,
		NumPartitions:     10,
		ReplicationFactor: 1,
		ConfigEntries:     config,
	}

	topic := components.Topic{}.New(topicConfig)
	if !topic.TopicExists(a.Topic, brokerAddress) {
		topic.CreateTopic(brokerAddress)
	}
}

func (a *App) createBackOffStrategy() *backoffStr.ExponentialBackOffWithRetries {
	config := backoffStr.ExponentialBackOffWithRetriesConfig{
		InitialInterval:     250 * time.Millisecond,
		RandomizationFactor: 0.5,
		Multiplier:          1.5,
		MaxInterval:         900 * time.Millisecond,
		MaxElapsedTime:      1500 * time.Millisecond,
		Stop:                -1,
		Clock:               backoff.SystemClock,
	}

	return backoffStr.ExponentialBackOffWithRetries{}.New(3, config)
}

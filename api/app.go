package api

import (
	"context"
	"errors"
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
	Port               string
	Topic              string
	BrokerAddress      string
	DestinationTimeout time.Duration
	Destinations       []mocks.Destination
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
		RequiredAcks: kafka.RequireAll, // wait for all kafka nodes to acknowledge the writes
		Logger:       log.New(os.Stdout, "kafka writer: ", 0),
	}

	return components.Producer{}.New(a.Topic, a.BrokerAddress, producerConfig)
}

func (a *App) createAndStartConsumers() {
	for i, _ := range a.Destinations {
		consumerConfig := components.ConsumerConfig{
			GroupID:     "event-delivery-kafka-" + a.Destinations[i].Name(), //different group Id for each consumer. Destination name should be unique
			MinBytes:    10e3,                                             // 10KB
			MaxBytes:    10e6,                                             // 10MB
			StartOffset: kafka.FirstOffset,
			Logger:      log.New(os.Stdout, fmt.Sprintf("kafka reader for groupId : event-delivery-kafka-%s", a.Destinations[i].Name()), 0),
		}

		backoffStrategy := a.createBackOffStrategy()

		consumer := components.Consumer{}.New(
			a.Topic,
			a.BrokerAddress,
			consumerConfig,
			processors.Processor{}.New(a.createConsumerAction(a.Destinations[i])),
			*backoffStrategy,
		)
		go consumer.Consume(context.Background())
	}
}

func (a *App) createConsumerAction(dest mocks.Destination) func(message kafka.Message) error {
	return func(message kafka.Message) error {
		ev := models.Event{}.New(string(message.Key), string(message.Value))

		result := make(chan error, 1)
		go func() {
			result <- dest.Receive(*ev)
		}()

		select {
		case <-time.After(a.DestinationTimeout):
			log.Printf("failed to send message: %v for key %s \n", dest.Name()+" : timed out", string(message.Key))
			return errors.New(dest.Name() + " : timed out")
		case res := <-result:
			if res != nil {
				log.Printf("failed to send message: %v for key %s \n", res.Error(), string(message.Key))
				return errors.New(res.Error())
			}
			return nil
		}
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

/*
Custom implementation of exponential backoff strategy to achieve the following time periods to choose randomly when to
run the next try. With existing implementation, use custom properties is not allowed.
[0.125sec , 0.375sec]	1st retry
[0.1875sec , 0.5625sec]	2nd retry
[0.2812sec , 0.843sec]	3rd retry

For more information, check documentation in github.com/cenkalti/backoff/v4@v4.1.3/exponential.go:16
*/
func (a *App) createBackOffStrategy() *backoffStr.ExponentialBackOffWithRetries {
	config := backoffStr.ExponentialBackOffWithRetriesConfig{
		InitialInterval:     250 * time.Millisecond,
		RandomizationFactor: 0.5,
		Multiplier:          1.5,
		MaxInterval:         900 * time.Millisecond,
		MaxElapsedTime:      4 * time.Second,
		Stop:                -1,
		Clock:               backoff.SystemClock,
	}

	return backoffStr.ExponentialBackOffWithRetries{}.New(3, config)
}

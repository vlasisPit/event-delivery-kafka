package api

import (
	"event-delivery-kafka/api/server"
	"event-delivery-kafka/kafka/components"
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

func (a *App) checkIfTopicExistsAndCreate(brokerAddress string) {
	topicConfig := components.TopicConfig{
		Topic:             a.Topic,
		NumPartitions:     10,
		ReplicationFactor: 1,
	}

	topic := components.Topic{}.New(topicConfig)
	if !topic.TopicExists(a.Topic, brokerAddress) {
		topic.CreateTopic(brokerAddress)
	}
}
package api

import (
	"context"
	"errors"
	"event-delivery-kafka/delivery/destinations/mocks"
	"event-delivery-kafka/kafka/components"
	"event-delivery-kafka/models"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	compose := setup()
	code := m.Run()
	shutdown(compose)
	os.Exit(code)
}

func setup() *testcontainers.LocalDockerCompose {
	err := godotenv.Load("../config/.env")

	if err != nil {
		log.Fatalf("Error loading .env file")
	}

	compose := testcontainers.NewLocalDockerCompose(
		[]string{"../docker-compose.yml"},
		strings.ToLower(uuid.New().String()),
	)
	compose.WithCommand([]string{"up", "-d"}).Invoke()
	time.Sleep(10 * time.Second)
	return compose
}

func shutdown(compose *testcontainers.LocalDockerCompose) {
	compose.Down()
	time.Sleep(1 * time.Second)
}

type RequestReceived struct {
	UserId string
	Status string
}

type DestinationMock struct {
	NameDest                string
	Count                   int
	RequestsReceivedHistory []RequestReceived
	Runner                  func(args ...int) error
}

func (des *DestinationMock) Receive(event ...models.Event) error {
	des.Count = des.Count + 1
	requestReceived := RequestReceived{UserId: event[0].UserID}

	err := des.Runner(des.Count)
	if err != nil {
		if err.Error() == "timeout" {
			requestReceived.Status = "timeout"
		} else {
			requestReceived.Status = "failed"
		}
	} else {
		requestReceived.Status = "success"
	}

	des.RequestsReceivedHistory = append(des.RequestsReceivedHistory, requestReceived)
	return err
}

func (des *DestinationMock) Name() string {
	return des.NameDest
}

/*
GIVEN
Destination fails for each request.

WHEN
Two events produced to Kafka

THEN
4 attempts (1 and 3 retires) for each event to sent to destination
*/
func TestDestinationFailedRepeatedly(t *testing.T) {
	runner := func(args ...int) error {
		return errors.New(" failed ...")
	}

	des := DestinationMock{
		NameDest:                "destination_failed" + uuid.New().String(),
		Count:                   0,
		RequestsReceivedHistory: nil,
		Runner:                  runner,
	}

	destinations := []mocks.Destination{&des}

	topicName := uuid.New().String() //unique topic name for each test
	app := App{
		Port:               os.Getenv("PORT"),
		Topic:              topicName,
		BrokerAddress:      os.Getenv("BROKER_ADDRESS"),
		DestinationTimeout: 1 * time.Second,
		Destinations:       destinations,
	}

	assert.Equal(t, true, createTopic(topicName, os.Getenv("BROKER_ADDRESS")))

	app.createAndStartConsumers()
	producer := app.createProducer()

	kafkaMessage1 := kafka.Message{
		Key:   []byte("user_test_1"),
		Value: []byte("event click 1"),
		Time:  time.Now(),
	}

	kafkaMessage2 := kafka.Message{
		Key:   []byte("user_test_2"),
		Value: []byte("event click 2"),
		Time:  time.Now(),
	}

	_ = producer.Writer.WriteMessages(context.Background(), kafkaMessage1, kafkaMessage2)

	expectedCount := 8

	//wait max 60 sec and check. If destination has received requests then break
	for i := 0; i < 60; i++ {
		if des.Count >= expectedCount {
			time.Sleep(1 * time.Second)
			break
		}
		time.Sleep(1 * time.Second)
	}

	assert.Equal(t, expectedCount, des.Count)

	//1st attempt for key user_test_1
	assert.Equal(t, "user_test_1", des.RequestsReceivedHistory[0].UserId)
	assert.Equal(t, "failed", des.RequestsReceivedHistory[0].Status)

	//2nd attempt for key user_test_1
	assert.Equal(t, "user_test_1", des.RequestsReceivedHistory[1].UserId)
	assert.Equal(t, "failed", des.RequestsReceivedHistory[1].Status)

	//3rd attempt for key user_test_1
	assert.Equal(t, "user_test_1", des.RequestsReceivedHistory[2].UserId)
	assert.Equal(t, "failed", des.RequestsReceivedHistory[2].Status)

	//4th attempt for key user_test_1
	assert.Equal(t, "user_test_1", des.RequestsReceivedHistory[3].UserId)
	assert.Equal(t, "failed", des.RequestsReceivedHistory[3].Status)

	//1st attempt for key user_test_2
	assert.Equal(t, "user_test_2", des.RequestsReceivedHistory[4].UserId)
	assert.Equal(t, "failed", des.RequestsReceivedHistory[4].Status)

	//2nd attempt for key user_test_2
	assert.Equal(t, "user_test_2", des.RequestsReceivedHistory[5].UserId)
	assert.Equal(t, "failed", des.RequestsReceivedHistory[5].Status)

	//3rd attempt for key user_test_2
	assert.Equal(t, "user_test_2", des.RequestsReceivedHistory[6].UserId)
	assert.Equal(t, "failed", des.RequestsReceivedHistory[6].Status)

	//4th attempt for key user_test_2
	assert.Equal(t, "user_test_2", des.RequestsReceivedHistory[7].UserId)
	assert.Equal(t, "failed", des.RequestsReceivedHistory[7].Status)
}

/*
GIVEN
2 Destinations.
The first fails for each request.
The seconds succeed for every request

WHEN
Three events produced to Kafka

THEN
12 attempts (1 and 3 retires) for each event to sent to destination with failures
3 attempts for each event to sent to destination WITHOUT failures
*/
func TestDestinationFailedRepeatedlyAndSecondDestinationSucceed(t *testing.T) {
	destWithFailures := DestinationMock{
		NameDest:                "destination_failed" + uuid.New().String(),
		Count:                   0,
		RequestsReceivedHistory: nil,
		Runner: func(args ...int) error {
			return errors.New(" failed ...")
		},
	}

	destWithSuccesses := DestinationMock{
		NameDest:                "destination_succeed" + uuid.New().String(),
		Count:                   0,
		RequestsReceivedHistory: nil,
		Runner: func(args ...int) error {
			return nil
		},
	}

	destinations := []mocks.Destination{&destWithSuccesses, &destWithFailures}

	topicName := uuid.New().String() //unique topic name for each test
	app := App{
		Port:               os.Getenv("PORT"),
		Topic:              topicName,
		BrokerAddress:      os.Getenv("BROKER_ADDRESS"),
		DestinationTimeout: 1 * time.Second,
		Destinations:       destinations,
	}

	assert.Equal(t, true, createTopic(topicName, os.Getenv("BROKER_ADDRESS")))

	app.createAndStartConsumers()
	producer := app.createProducer()

	kafkaMessage1 := kafka.Message{
		Key:   []byte("user_test_1"),
		Value: []byte("event click 1"),
		Time:  time.Now(),
	}

	kafkaMessage2 := kafka.Message{
		Key:   []byte("user_test_2"),
		Value: []byte("event click 2"),
		Time:  time.Now(),
	}

	kafkaMessage3 := kafka.Message{
		Key:   []byte("user_test_3"),
		Value: []byte("event click 3"),
		Time:  time.Now(),
	}

	_ = producer.Writer.WriteMessages(context.Background(), kafkaMessage1, kafkaMessage2, kafkaMessage3)

	expectedSuccessCount := 3
	expectedFailuresCount := 12

	//wait max 60 sec and check. If destination has received requests then break
	for i := 0; i < 60; i++ {
		if destWithSuccesses.Count >= expectedSuccessCount && destWithFailures.Count >= expectedFailuresCount {
			time.Sleep(1 * time.Second)
			break
		}
		time.Sleep(1 * time.Second)
	}

	assert.Equal(t, expectedFailuresCount, destWithFailures.Count)
	assert.Equal(t, expectedSuccessCount, destWithSuccesses.Count)

	//Success for key user_test_1
	assert.Equal(t, "user_test_1", destWithSuccesses.RequestsReceivedHistory[0].UserId)
	assert.Equal(t, "success", destWithSuccesses.RequestsReceivedHistory[0].Status)

	//Success for key user_test_2
	assert.Equal(t, "user_test_2", destWithSuccesses.RequestsReceivedHistory[1].UserId)
	assert.Equal(t, "success", destWithSuccesses.RequestsReceivedHistory[1].Status)

	//Success for key user_test_3
	assert.Equal(t, "user_test_3", destWithSuccesses.RequestsReceivedHistory[2].UserId)
	assert.Equal(t, "success", destWithSuccesses.RequestsReceivedHistory[2].Status)

	//1st attempt for key user_test_1
	assert.Equal(t, "user_test_1", destWithFailures.RequestsReceivedHistory[0].UserId)
	assert.Equal(t, "failed", destWithFailures.RequestsReceivedHistory[0].Status)

	//2nd attempt for key user_test_1
	assert.Equal(t, "user_test_1", destWithFailures.RequestsReceivedHistory[1].UserId)
	assert.Equal(t, "failed", destWithFailures.RequestsReceivedHistory[1].Status)

	//3rd attempt for key user_test_1
	assert.Equal(t, "user_test_1", destWithFailures.RequestsReceivedHistory[2].UserId)
	assert.Equal(t, "failed", destWithFailures.RequestsReceivedHistory[2].Status)

	//4th attempt for key user_test_1
	assert.Equal(t, "user_test_1", destWithFailures.RequestsReceivedHistory[3].UserId)
	assert.Equal(t, "failed", destWithFailures.RequestsReceivedHistory[3].Status)

	//1st attempt for key user_test_2
	assert.Equal(t, "user_test_2", destWithFailures.RequestsReceivedHistory[4].UserId)
	assert.Equal(t, "failed", destWithFailures.RequestsReceivedHistory[4].Status)

	//2nd attempt for key user_test_2
	assert.Equal(t, "user_test_2", destWithFailures.RequestsReceivedHistory[5].UserId)
	assert.Equal(t, "failed", destWithFailures.RequestsReceivedHistory[5].Status)

	//3rd attempt for key user_test_2
	assert.Equal(t, "user_test_2", destWithFailures.RequestsReceivedHistory[6].UserId)
	assert.Equal(t, "failed", destWithFailures.RequestsReceivedHistory[6].Status)

	//4th attempt for key user_test_2
	assert.Equal(t, "user_test_2", destWithFailures.RequestsReceivedHistory[7].UserId)
	assert.Equal(t, "failed", destWithFailures.RequestsReceivedHistory[7].Status)

	//1st attempt for key user_test_3
	assert.Equal(t, "user_test_3", destWithFailures.RequestsReceivedHistory[8].UserId)
	assert.Equal(t, "failed", destWithFailures.RequestsReceivedHistory[8].Status)

	//2nd attempt for key user_test_3
	assert.Equal(t, "user_test_3", destWithFailures.RequestsReceivedHistory[9].UserId)
	assert.Equal(t, "failed", destWithFailures.RequestsReceivedHistory[9].Status)

	//3rd attempt for key user_test_3
	assert.Equal(t, "user_test_3", destWithFailures.RequestsReceivedHistory[10].UserId)
	assert.Equal(t, "failed", destWithFailures.RequestsReceivedHistory[10].Status)

	//4th attempt for key user_test_3
	assert.Equal(t, "user_test_3", destWithFailures.RequestsReceivedHistory[11].UserId)
	assert.Equal(t, "failed", destWithFailures.RequestsReceivedHistory[11].Status)
}

/*
GIVEN
Destination fails for each request because of timeout error

WHEN
One event produced to Kafka

THEN
4 attempts (1 and 3 retires) to deliver to destination
*/
func TestDestinationFailedRepeatedlyTimeout(t *testing.T) {
	runner := func(args ...int) error {
		time.Sleep(80 * time.Millisecond)
		return errors.New("timeout")
	}

	des := DestinationMock{
		NameDest:                "destination_failed_timeout" + uuid.New().String(),
		Count:                   0,
		RequestsReceivedHistory: nil,
		Runner:                  runner,
	}

	destinations := []mocks.Destination{&des}

	topicName := uuid.New().String() //unique topic name for each test
	app := App{
		Port:               os.Getenv("PORT"),
		Topic:              topicName,
		BrokerAddress:      os.Getenv("BROKER_ADDRESS"),
		DestinationTimeout: 50 * time.Millisecond,
		Destinations:       destinations,
	}

	assert.Equal(t, true, createTopic(topicName, os.Getenv("BROKER_ADDRESS")))

	app.createAndStartConsumers()
	producer := app.createProducer()

	kafkaMessage1 := kafka.Message{
		Key:   []byte("user_test_1"),
		Value: []byte("event click 1"),
		Time:  time.Now(),
	}

	_ = producer.Writer.WriteMessages(context.Background(), kafkaMessage1)

	expectedCount := 4

	//wait max 60 sec and check. If destination has received requests then break
	for i := 0; i < 60; i++ {
		if des.Count >= expectedCount {
			time.Sleep(1 * time.Second)
			break
		}
		time.Sleep(1 * time.Second)
	}

	assert.Equal(t, expectedCount, des.Count)

	//1st attempt for key user_test_1
	assert.Equal(t, "user_test_1", des.RequestsReceivedHistory[0].UserId)
	assert.Equal(t, "timeout", des.RequestsReceivedHistory[0].Status)

	//2nd attempt for key user_test_1
	assert.Equal(t, "user_test_1", des.RequestsReceivedHistory[1].UserId)
	assert.Equal(t, "timeout", des.RequestsReceivedHistory[1].Status)

	//3rd attempt for key user_test_1
	assert.Equal(t, "user_test_1", des.RequestsReceivedHistory[2].UserId)
	assert.Equal(t, "timeout", des.RequestsReceivedHistory[2].Status)

	//4th attempt for key user_test_1
	assert.Equal(t, "user_test_1", des.RequestsReceivedHistory[3].UserId)
	assert.Equal(t, "timeout", des.RequestsReceivedHistory[3].Status)
}

/*
GIVEN
Destination fails for first request, but then after first retry, request succeed

WHEN
One event produced to Kafka

THEN
2 attempts (1 and 1 retry) to deliver to destination
*/
func TestDestinationFailedAndThenSucceed(t *testing.T) {
	runner := func(args ...int) error {
		if args[0] == 1 {
			return errors.New("error")
		} else {
			return nil
		}
	}

	des := DestinationMock{
		NameDest:                "destination_failed_and_succeed" + uuid.New().String(),
		Count:                   0,
		RequestsReceivedHistory: nil,
		Runner:                  runner,
	}

	destinations := []mocks.Destination{&des}

	topicName := uuid.New().String() //unique topic name for each test
	app := App{
		Port:               os.Getenv("PORT"),
		Topic:              topicName,
		BrokerAddress:      os.Getenv("BROKER_ADDRESS"),
		DestinationTimeout: 500 * time.Millisecond,
		Destinations:       destinations,
	}

	assert.Equal(t, true, createTopic(topicName, os.Getenv("BROKER_ADDRESS")))

	app.createAndStartConsumers()
	producer := app.createProducer()

	kafkaMessage1 := kafka.Message{
		Key:   []byte("user_test_1"),
		Value: []byte("event click 1"),
		Time:  time.Now(),
	}

	_ = producer.Writer.WriteMessages(context.Background(), kafkaMessage1)

	expectedCount := 2

	//wait max 60 sec and check. If destination has received requests then break
	for i := 0; i < 60; i++ {
		if des.Count >= expectedCount {
			time.Sleep(1 * time.Second)
			break
		}
		time.Sleep(1 * time.Second)
	}

	assert.Equal(t, expectedCount, des.Count)

	//1st attempt for key user_test_1
	assert.Equal(t, "user_test_1", des.RequestsReceivedHistory[0].UserId)
	assert.Equal(t, "failed", des.RequestsReceivedHistory[0].Status)

	//2nd attempt for key user_test_1
	assert.Equal(t, "user_test_1", des.RequestsReceivedHistory[1].UserId)
	assert.Equal(t, "success", des.RequestsReceivedHistory[1].Status)
}

/*
Topic should have ONLY ONE partition to preserve ordering of messages
to have the test passed
*/
func createTopic(name string, brokerAddress string) bool {
	config := []kafka.ConfigEntry{{
		ConfigName:  "log.retention.hours",
		ConfigValue: "24",
	}}

	topicConfig := components.TopicConfig{
		Topic:             name,
		NumPartitions:     1,
		ReplicationFactor: 1,
		ConfigEntries:     config,
	}

	topic := components.Topic{}.New(topicConfig)
	topic.CreateTopic(brokerAddress)

	for i := 0; i < 5; i++ {
		if !topic.TopicExists(name, brokerAddress) {
			topic.CreateTopic(brokerAddress)
		} else {
			break
		}
	}
	if !topic.TopicExists(name, brokerAddress) {
		return false
	}
	return true
}

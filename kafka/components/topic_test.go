package components

import (
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

func TestMain(m *testing.M) {
	compose := setup()
	code := m.Run()
	shutdown(compose)
	os.Exit(code)
}

func setup() *testcontainers.LocalDockerCompose {
	compose := testcontainers.NewLocalDockerCompose(
		[]string{"../../docker-compose.yml"},
		strings.ToLower(uuid.New().String()),
	)
	compose.WithCommand([]string{"up", "-d"}).Invoke()
	time.Sleep(5 * time.Second)
	return compose
}

func shutdown(compose *testcontainers.LocalDockerCompose) {
	compose.Down()
	time.Sleep(1 * time.Second)
}

func TestCreateAndListTopics(t *testing.T) {
	brokerAddress := "localhost:9092"

	config := []kafka.ConfigEntry{{
		ConfigName:  "log.retention.hours",
		ConfigValue: "24",
	}}

	topicConfig1 := TopicConfig{
		Topic:             "test-topic-1",
		NumPartitions:     10,
		ReplicationFactor: 1,
		ConfigEntries:     config,
	}

	topicConfig2 := TopicConfig{
		Topic:             "test-topic-2",
		NumPartitions:     10,
		ReplicationFactor: 1,
		ConfigEntries:     config,
	}

	topicConfig3 := TopicConfig{
		Topic:             "test-topic-3",
		NumPartitions:     10,
		ReplicationFactor: 1,
		ConfigEntries:     config,
	}

	topic1 := Topic{}.New(topicConfig1)
	topic2 := Topic{}.New(topicConfig2)
	topic3 := Topic{}.New(topicConfig3)

	topic1.CreateTopic(brokerAddress)
	topic2.CreateTopic(brokerAddress)

	time.Sleep(2 * time.Second)

	assert.Equal(t, true, topic1.TopicExists(topic1.config.Topic, brokerAddress))
	assert.Equal(t, true, topic2.TopicExists(topic2.config.Topic, brokerAddress))
	assert.Equal(t, false, topic3.TopicExists(topic3.config.Topic, brokerAddress))
}

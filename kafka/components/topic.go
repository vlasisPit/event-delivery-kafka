package components

import (
	"github.com/segmentio/kafka-go"
	"net"
	"strconv"
)

type TopicConfig struct {
	Topic             string
	NumPartitions     int
	ReplicationFactor int
	ConfigEntries     []kafka.ConfigEntry
}

type Topic struct {
	config TopicConfig
}

func (Topic) New(config TopicConfig) (p *Topic) {
	return &Topic{
		config: config,
	}
}

/*
App can not work if topic can not be created in Kafka.
I such a case, app should stop working (panic)
*/
func (topic *Topic) CreateTopic(brokerAddress string) {
	conn, err := kafka.Dial("tcp", brokerAddress)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic.config.Topic,
			NumPartitions:     topic.config.NumPartitions,
			ReplicationFactor: topic.config.ReplicationFactor,
			ConfigEntries:     topic.config.ConfigEntries,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}
}

func (topic *Topic) TopicExists(topicName string, brokerAddress string) bool {
	conn, err := kafka.Dial("tcp", brokerAddress)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		panic(err.Error())
	}

	m := map[string]struct{}{}

	for _, p := range partitions {
		m[p.Topic] = struct{}{}
	}

	if _, ok := m[topicName]; ok {
		return true
	}

	return false
}

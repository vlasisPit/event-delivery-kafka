package processors

import "github.com/segmentio/kafka-go"

type Processor struct {
	Action func(message kafka.Message) error
}

func (Processor) New(action func(message kafka.Message) error) *Processor {
	return &Processor{Action: action}
}
package backoff

import (
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"log"
	"os"
	"time"
)

type ExponentialBackOffWithRetriesConfig struct {
	InitialInterval     time.Duration
	RandomizationFactor float64
	Multiplier          float64
	MaxInterval         time.Duration
	MaxElapsedTime      time.Duration
	Stop                time.Duration
	Clock               backoff.Clock
}

type ExponentialBackOffWithRetries struct {
	backoffImpl *backoff.ExponentialBackOff
	maxRetries  uint64
	logger      *log.Logger
}

func (ExponentialBackOffWithRetries) New(maxRetries uint64, config ExponentialBackOffWithRetriesConfig) *ExponentialBackOffWithRetries {
	backoffImpl := &backoff.ExponentialBackOff{
		InitialInterval:     config.InitialInterval,
		RandomizationFactor: config.RandomizationFactor,
		Multiplier:          config.Multiplier,
		MaxInterval:         config.MaxInterval,
		MaxElapsedTime:      config.MaxElapsedTime,
		Stop:                config.Stop,
		Clock:               config.Clock,
	}
	backoffImpl.Reset()

	logger := log.New(os.Stdout, "custom exponential backoff runner: ", 0)

	return &ExponentialBackOffWithRetries{
		backoffImpl: backoffImpl,
		maxRetries:  maxRetries,
		logger:      logger,
	}
}

func (expBackoff *ExponentialBackOffWithRetries) Run(operation backoff.Operation) error {
	backoffWithMaxRetry := backoff.WithMaxRetries(expBackoff.backoffImpl, expBackoff.maxRetries)
	return backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		expBackoff.logger.Println(fmt.Sprintf("error: %v, retrying after %v seconds \n", err.Error(), t.Seconds()))
	})
}

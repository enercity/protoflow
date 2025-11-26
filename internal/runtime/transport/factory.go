package transport

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/enercity/protoflow/internal/runtime/config"
)

// Transport combines a publisher and subscriber pair produced by a factory.
type Transport struct {
	Publisher  message.Publisher
	Subscriber message.Subscriber
}

// Factory abstracts how Protoflow initialises message transports.
type Factory interface {
	Build(ctx context.Context, conf *config.Config, logger watermill.LoggerAdapter) (Transport, error)
}

// DefaultFactory returns the built-in transport factory that knows how to
// initialise AWS SNS/SQS and in-memory channel transports.
func DefaultFactory() Factory {
	return defaultFactory{}
}

type defaultFactory struct{}

func (defaultFactory) Build(ctx context.Context, conf *config.Config, logger watermill.LoggerAdapter) (Transport, error) {
	if conf == nil {
		return Transport{}, fmt.Errorf("config is required")
	}

	switch conf.PubSubSystem {
	case "aws":
		return awsTransport(ctx, conf, logger)
	case "channel":
		return channelTransport(conf, logger)
	default:
		return Transport{}, fmt.Errorf("unsupported PubSubSystem, must be 'aws' or 'channel'")
	}
}

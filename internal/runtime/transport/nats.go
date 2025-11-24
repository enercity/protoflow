package transport

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/enercity/protoflow/internal/runtime/config"
)

var (
	NATSPublisherFactory = func(cfg nats.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
		return nats.NewPublisher(cfg, logger)
	}
	NATSSubscriberFactory = func(cfg nats.SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {
		return nats.NewSubscriber(cfg, logger)
	}
)

func natsTransport(conf *config.Config, logger watermill.LoggerAdapter) (Transport, error) {
	marshaler := &nats.NATSMarshaler{}

	publisher, err := NATSPublisherFactory(
		nats.PublisherConfig{
			URL:       conf.NATSURL,
			Marshaler: marshaler,
		},
		logger,
	)
	if err != nil {
		return Transport{}, err
	}

	subscriber, err := NATSSubscriberFactory(
		nats.SubscriberConfig{
			URL:         conf.NATSURL,
			Unmarshaler: marshaler,
		},
		logger,
	)
	if err != nil {
		return Transport{}, err
	}

	return Transport{
		Publisher:  publisher,
		Subscriber: subscriber,
	}, nil
}

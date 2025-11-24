package transport

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"

	"github.com/enercity/protoflow/internal/runtime/config"
)

var (
	GoChannelFactory = func(cfg gochannel.Config, logger watermill.LoggerAdapter) (message.Publisher, message.Subscriber) {
		pubSub := gochannel.NewGoChannel(cfg, logger)
		return pubSub, pubSub
	}
)

func channelTransport(conf *config.Config, logger watermill.LoggerAdapter) (Transport, error) {
	pub, sub := GoChannelFactory(gochannel.Config{}, logger)

	return Transport{
		Publisher:  pub,
		Subscriber: sub,
	}, nil
}

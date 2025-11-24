package transport

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/enercity/protoflow/internal/runtime/config"
)

var (
	KafkaPublisherFactory = func(cfg kafka.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
		return kafka.NewPublisher(cfg, logger)
	}
	KafkaSubscriberFactory = func(cfg kafka.SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {
		return kafka.NewSubscriber(cfg, logger)
	}
)

func kafkaTransport(conf *config.Config, logger watermill.LoggerAdapter) (Transport, error) {
	publisher, err := newKafkaPublisher(conf.KafkaBrokers, logger)
	if err != nil {
		return Transport{}, err
	}
	subscriber, err := newKafkaSubscriber(conf.KafkaConsumerGroup, conf.KafkaBrokers, logger)
	if err != nil {
		return Transport{}, err
	}
	return Transport{Publisher: publisher, Subscriber: subscriber}, nil
}

func newKafkaPublisher(brokers []string, logger watermill.LoggerAdapter) (message.Publisher, error) {
	return KafkaPublisherFactory(
		kafka.PublisherConfig{
			Brokers:   brokers,
			Marshaler: kafka.DefaultMarshaler{},
		},
		logger,
	)
}

func newKafkaSubscriber(consumerGroup string, brokers []string, logger watermill.LoggerAdapter) (message.Subscriber, error) {
	return KafkaSubscriberFactory(
		kafka.SubscriberConfig{
			Brokers:       brokers,
			Unmarshaler:   kafka.DefaultMarshaler{},
			ConsumerGroup: consumerGroup,
		},
		logger,
	)
}

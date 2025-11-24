package transport

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v3/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/enercity/protoflow/internal/runtime/config"
)

var (
	AmqpConnectionFactory = func(cfg amqp.ConnectionConfig, logger watermill.LoggerAdapter) (*amqp.ConnectionWrapper, error) {
		return amqp.NewConnection(cfg, logger)
	}
	AmqpPublisherFactory = func(cfg amqp.Config, logger watermill.LoggerAdapter, conn *amqp.ConnectionWrapper) (message.Publisher, error) {
		return amqp.NewPublisherWithConnection(cfg, logger, conn)
	}
	AmqpSubscriberFactory = func(cfg amqp.Config, logger watermill.LoggerAdapter, conn *amqp.ConnectionWrapper) (message.Subscriber, error) {
		return amqp.NewSubscriberWithConnection(cfg, logger, conn)
	}
)

func rabbitTransport(conf *config.Config, logger watermill.LoggerAdapter) (Transport, error) {
	conn, amqpConfig, err := setupAmqp(conf, logger)
	if err != nil {
		return Transport{}, err
	}
	publisher, err := newRabbitMQPublisher(amqpConfig, conn, logger)
	if err != nil {
		return Transport{}, err
	}
	subscriber, err := newRabbitMQSubscriber(amqpConfig, conn, logger)
	if err != nil {
		return Transport{}, err
	}
	return Transport{Publisher: publisher, Subscriber: subscriber}, nil
}

func setupAmqp(conf *config.Config, logger watermill.LoggerAdapter) (*amqp.ConnectionWrapper, amqp.Config, error) {
	ampqConfig := amqp.NewDurablePubSubConfig(
		conf.RabbitMQURL,
		amqp.GenerateQueueNameTopicNameWithSuffix("-queueSuffix"),
	)
	ampqConn, err := AmqpConnectionFactory(amqp.ConnectionConfig{
		AmqpURI:   conf.RabbitMQURL,
		TLSConfig: nil,
		Reconnect: amqp.DefaultReconnectConfig(),
	}, logger)
	if err != nil {
		return nil, amqp.Config{}, err
	}
	return ampqConn, ampqConfig, nil
}

func newRabbitMQPublisher(cfg amqp.Config, conn *amqp.ConnectionWrapper, logger watermill.LoggerAdapter) (message.Publisher, error) {
	return AmqpPublisherFactory(cfg, logger, conn)
}

func newRabbitMQSubscriber(cfg amqp.Config, conn *amqp.ConnectionWrapper, logger watermill.LoggerAdapter) (message.Subscriber, error) {
	return AmqpSubscriberFactory(cfg, logger, conn)
}

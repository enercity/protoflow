package transport

import (
	"errors"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/enercity/protoflow/internal/runtime/config"
)

func TestNewKafkaPublisherReturnsError(t *testing.T) {
	orig := KafkaPublisherFactory
	t.Cleanup(func() { KafkaPublisherFactory = orig })

	KafkaPublisherFactory = func(cfg kafka.PublisherConfig, _ watermill.LoggerAdapter) (message.Publisher, error) {
		return nil, errors.New("pub")
	}

	if _, err := newKafkaPublisher([]string{"broker"}, watermill.NopLogger{}); err == nil {
		t.Fatal("expected error when kafka publisher creation fails")
	}
}

func TestNewKafkaSubscriberReturnsError(t *testing.T) {
	orig := KafkaSubscriberFactory
	t.Cleanup(func() { KafkaSubscriberFactory = orig })

	KafkaSubscriberFactory = func(cfg kafka.SubscriberConfig, _ watermill.LoggerAdapter) (message.Subscriber, error) {
		return nil, errors.New("sub")
	}

	if _, err := newKafkaSubscriber("group", []string{"broker"}, watermill.NopLogger{}); err == nil {
		t.Fatal("expected error when kafka subscriber creation fails")
	}
}

func TestKafkaTransportFailsOnPublisherError(t *testing.T) {
	origFactory := KafkaPublisherFactory
	defer func() { KafkaPublisherFactory = origFactory }()

	KafkaPublisherFactory = func(cfg kafka.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
		return nil, errors.New("publisher fail")
	}

	_, err := kafkaTransport(&config.Config{}, watermill.NopLogger{})
	if err == nil {
		t.Fatal("expected error when publisher factory fails")
	}
}

func TestKafkaTransportFailsOnSubscriberError(t *testing.T) {
	origFactory := KafkaSubscriberFactory
	defer func() { KafkaSubscriberFactory = origFactory }()

	KafkaSubscriberFactory = func(cfg kafka.SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {
		return nil, errors.New("subscriber fail")
	}

	// We need publisher to succeed
	origPubFactory := KafkaPublisherFactory
	defer func() { KafkaPublisherFactory = origPubFactory }()
	KafkaPublisherFactory = func(cfg kafka.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
		return &testPublisher{}, nil
	}

	_, err := kafkaTransport(&config.Config{}, watermill.NopLogger{})
	if err == nil {
		t.Fatal("expected error when subscriber factory fails")
	}
}

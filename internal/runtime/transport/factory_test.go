package transport

import (
	"context"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v3/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill-aws/sns"
	"github.com/ThreeDotsLabs/watermill-aws/sqs"
	"github.com/ThreeDotsLabs/watermill-http/v2/pkg/http"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"github.com/enercity/protoflow/internal/runtime/config"
)

func TestKafkaTransportReturnsPublisherAndSubscriber(t *testing.T) {
	origPub := KafkaPublisherFactory
	origSub := KafkaSubscriberFactory
	t.Cleanup(func() {
		KafkaPublisherFactory = origPub
		KafkaSubscriberFactory = origSub
	})

	pub := &testPublisher{}
	sub := &testSubscriber{}

	KafkaPublisherFactory = func(cfg kafka.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
		if len(cfg.Brokers) != 1 || cfg.Brokers[0] != "broker" {
			t.Fatalf("unexpected brokers: %#v", cfg.Brokers)
		}
		return pub, nil
	}
	KafkaSubscriberFactory = func(cfg kafka.SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {
		if cfg.ConsumerGroup != "group" {
			t.Fatalf("unexpected consumer group %s", cfg.ConsumerGroup)
		}
		return sub, nil
	}

	transport, err := kafkaTransport(&config.Config{KafkaBrokers: []string{"broker"}, KafkaConsumerGroup: "group"}, watermill.NopLogger{})
	if err != nil {
		t.Fatalf("unexpected kafka transport error: %v", err)
	}
	if transport.Publisher != pub || transport.Subscriber != sub {
		t.Fatal("expected kafka transport to expose publisher and subscriber")
	}
}

func TestRabbitTransportReturnsPublisherAndSubscriber(t *testing.T) {
	origConn := AmqpConnectionFactory
	origPub := AmqpPublisherFactory
	origSub := AmqpSubscriberFactory
	t.Cleanup(func() {
		AmqpConnectionFactory = origConn
		AmqpPublisherFactory = origPub
		AmqpSubscriberFactory = origSub
	})

	conn := &amqp.ConnectionWrapper{}
	pub := &testPublisher{}
	sub := &testSubscriber{}

	AmqpConnectionFactory = func(cfg amqp.ConnectionConfig, logger watermill.LoggerAdapter) (*amqp.ConnectionWrapper, error) {
		if cfg.AmqpURI == "" {
			t.Fatal("expected AMQP URI to be set")
		}
		return conn, nil
	}
	AmqpPublisherFactory = func(cfg amqp.Config, logger watermill.LoggerAdapter, c *amqp.ConnectionWrapper) (message.Publisher, error) {
		if c != conn {
			t.Fatalf("unexpected connection instance")
		}
		return pub, nil
	}
	AmqpSubscriberFactory = func(cfg amqp.Config, logger watermill.LoggerAdapter, c *amqp.ConnectionWrapper) (message.Subscriber, error) {
		if c != conn {
			t.Fatalf("unexpected connection instance")
		}
		return sub, nil
	}

	transport, err := rabbitTransport(&config.Config{RabbitMQURL: "amqp://guest"}, watermill.NopLogger{})
	if err != nil {
		t.Fatalf("unexpected rabbit transport error: %v", err)
	}
	if transport.Publisher != pub || transport.Subscriber != sub {
		t.Fatal("expected rabbit transport components to be returned")
	}
}

func TestAwsTransportUsesCustomFactories(t *testing.T) {
	origLoader := AWSDefaultConfigLoader
	origTopic := SNSTopicResolverFactory
	origPub := SNSPublisherFactory
	origSub := SNSSubscriberFactory
	t.Cleanup(func() {
		AWSDefaultConfigLoader = origLoader
		SNSTopicResolverFactory = origTopic
		SNSPublisherFactory = origPub
		SNSSubscriberFactory = origSub
	})

	AWSDefaultConfigLoader = func(ctx context.Context, optFns ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{Region: "us-east-1"}, nil
	}
	SNSTopicResolverFactory = func(accountID, region string) (*sns.GenerateArnTopicResolver, error) {
		return origTopic(accountID, region)
	}

	pub := &testPublisher{}
	sub := &testSubscriber{}

	SNSPublisherFactory = func(cfg sns.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
		if cfg.TopicResolver == nil {
			t.Fatal("topic resolver must be set")
		}
		return pub, nil
	}
	SNSSubscriberFactory = func(cfg sns.SubscriberConfig, sqsCfg sqs.SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {
		return sub, nil
	}

	conf := &config.Config{PubSubSystem: "aws", AWSAccountID: "000000000000", AWSRegion: "eu-west-1"}
	transport, err := awsTransport(context.Background(), conf, watermill.NopLogger{})
	if err != nil {
		t.Fatalf("unexpected aws transport error: %v", err)
	}
	if transport.Publisher != pub || transport.Subscriber != sub {
		t.Fatal("expected aws transport components to be returned")
	}
}

func TestDefaultFactoryBuild(t *testing.T) {
	factory := DefaultFactory()

	t.Run("unsupported system", func(t *testing.T) {
		if _, err := factory.Build(context.Background(), &config.Config{PubSubSystem: "unknown"}, watermill.NopLogger{}); err == nil {
			t.Fatal("expected error for unknown pubsub system")
		}
	})

	for _, tc := range []factoryBuildCase{kafkaFactoryCase(), rabbitFactoryCase(), awsFactoryCase(), natsFactoryCase(), channelFactoryCase(), ioFactoryCase(), httpFactoryCase()} {
		t.Run(tc.name, func(t *testing.T) {
			cleanup, expectedPub, expectedSub := tc.setup(t)
			if cleanup != nil {
				t.Cleanup(cleanup)
			}
			transport, err := factory.Build(context.Background(), tc.cfg, watermill.NopLogger{})
			if err != nil {
				t.Fatalf("unexpected error building %s transport: %v", tc.name, err)
			}
			if transport.Publisher != expectedPub || transport.Subscriber != expectedSub {
				t.Fatalf("expected %s transport to reuse stub components", tc.name)
			}
		})
	}
}

func httpFactoryCase() factoryBuildCase {
	return factoryBuildCase{
		name: "http",
		cfg:  &config.Config{PubSubSystem: "http", HTTPServerAddress: ":8080", HTTPPublisherURL: "http://localhost:8080"},
		setup: func(t *testing.T) (func(), message.Publisher, message.Subscriber) {
			t.Helper()
			origPub := HTTPPublisherFactory
			origSub := HTTPSubscriberFactory
			pub := &testPublisher{}
			sub := &testSubscriber{}
			HTTPPublisherFactory = func(config http.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
				return pub, nil
			}
			HTTPSubscriberFactory = func(addr string, config http.SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {
				if addr != ":8080" {
					t.Fatal("unexpected address")
				}
				return sub, nil
			}
			return func() {
				HTTPPublisherFactory = origPub
				HTTPSubscriberFactory = origSub
			}, pub, sub
		},
	}
}

func natsFactoryCase() factoryBuildCase {
	return factoryBuildCase{
		name: "nats",
		cfg:  &config.Config{PubSubSystem: "nats", NATSURL: "nats://localhost:4222"},
		setup: func(t *testing.T) (func(), message.Publisher, message.Subscriber) {
			t.Helper()
			origPub := NATSPublisherFactory
			origSub := NATSSubscriberFactory
			pub := &testPublisher{}
			sub := &testSubscriber{}
			NATSPublisherFactory = func(cfg nats.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
				if cfg.URL != "nats://localhost:4222" {
					t.Fatal("unexpected NATS URL")
				}
				return pub, nil
			}
			NATSSubscriberFactory = func(cfg nats.SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {
				if cfg.URL != "nats://localhost:4222" {
					t.Fatal("unexpected NATS URL")
				}
				return sub, nil
			}
			return func() {
				NATSPublisherFactory = origPub
				NATSSubscriberFactory = origSub
			}, pub, sub
		},
	}
}

func channelFactoryCase() factoryBuildCase {
	return factoryBuildCase{
		name: "channel",
		cfg:  &config.Config{PubSubSystem: "channel"},
		setup: func(t *testing.T) (func(), message.Publisher, message.Subscriber) {
			t.Helper()
			origFactory := GoChannelFactory
			pub := &testPublisher{}
			sub := &testSubscriber{}
			GoChannelFactory = func(cfg gochannel.Config, logger watermill.LoggerAdapter) (message.Publisher, message.Subscriber) {
				return pub, sub
			}
			return func() {
				GoChannelFactory = origFactory
			}, pub, sub
		},
	}
}

func ioFactoryCase() factoryBuildCase {
	return factoryBuildCase{
		name: "io",
		cfg:  &config.Config{PubSubSystem: "io", IOFile: "test.log"},
		setup: func(t *testing.T) (func(), message.Publisher, message.Subscriber) {
			t.Helper()
			origPub := IOPublisherFactory
			origSub := IOSubscriberFactory
			pub := &testPublisher{}
			sub := &testSubscriber{}
			IOPublisherFactory = func(filePath string, logger watermill.LoggerAdapter) (message.Publisher, error) {
				if filePath != "test.log" {
					t.Fatal("unexpected file path")
				}
				return pub, nil
			}
			IOSubscriberFactory = func(filePath string, logger watermill.LoggerAdapter) (message.Subscriber, error) {
				if filePath != "test.log" {
					t.Fatal("unexpected file path")
				}
				return sub, nil
			}
			return func() {
				IOPublisherFactory = origPub
				IOSubscriberFactory = origSub
			}, pub, sub
		},
	}
}

func TestDefaultFactoryRequiresConfig(t *testing.T) {
	if _, err := (defaultFactory{}).Build(context.Background(), nil, watermill.NopLogger{}); err == nil {
		t.Fatal("expected error when config nil")
	}
}

type factoryBuildCase struct {
	name  string
	cfg   *config.Config
	setup func(t *testing.T) (cleanup func(), pub message.Publisher, sub message.Subscriber)
}

func kafkaFactoryCase() factoryBuildCase {
	return factoryBuildCase{
		name: "kafka",
		cfg:  &config.Config{PubSubSystem: "kafka", KafkaBrokers: []string{"broker"}, KafkaConsumerGroup: "group"},
		setup: func(t *testing.T) (func(), message.Publisher, message.Subscriber) {
			t.Helper()
			origPub := KafkaPublisherFactory
			origSub := KafkaSubscriberFactory
			pub := &testPublisher{}
			sub := &testSubscriber{}
			KafkaPublisherFactory = func(cfg kafka.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
				if len(cfg.Brokers) == 0 {
					t.Fatal("expected brokers to be provided")
				}
				return pub, nil
			}
			KafkaSubscriberFactory = func(cfg kafka.SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {
				if cfg.ConsumerGroup == "" {
					t.Fatal("expected consumer group to be provided")
				}
				return sub, nil
			}
			return func() {
				KafkaPublisherFactory = origPub
				KafkaSubscriberFactory = origSub
			}, pub, sub
		},
	}
}

func rabbitFactoryCase() factoryBuildCase {
	return factoryBuildCase{
		name: "rabbitmq",
		cfg:  &config.Config{PubSubSystem: "rabbitmq", RabbitMQURL: "amqp://guest"},
		setup: func(t *testing.T) (func(), message.Publisher, message.Subscriber) {
			t.Helper()
			origConn := AmqpConnectionFactory
			origPub := AmqpPublisherFactory
			origSub := AmqpSubscriberFactory
			conn := &amqp.ConnectionWrapper{}
			pub := &testPublisher{}
			sub := &testSubscriber{}
			AmqpConnectionFactory = func(cfg amqp.ConnectionConfig, logger watermill.LoggerAdapter) (*amqp.ConnectionWrapper, error) {
				if cfg.AmqpURI == "" {
					t.Fatal("expected AMQP URI")
				}
				return conn, nil
			}
			AmqpPublisherFactory = func(cfg amqp.Config, logger watermill.LoggerAdapter, c *amqp.ConnectionWrapper) (message.Publisher, error) {
				if c != conn {
					t.Fatal("unexpected connection passed to publisher factory")
				}
				return pub, nil
			}
			AmqpSubscriberFactory = func(cfg amqp.Config, logger watermill.LoggerAdapter, c *amqp.ConnectionWrapper) (message.Subscriber, error) {
				if c != conn {
					t.Fatal("unexpected connection passed to subscriber factory")
				}
				return sub, nil
			}
			return func() {
				AmqpConnectionFactory = origConn
				AmqpPublisherFactory = origPub
				AmqpSubscriberFactory = origSub
			}, pub, sub
		},
	}
}

func awsFactoryCase() factoryBuildCase {
	return factoryBuildCase{
		name: "aws",
		cfg:  &config.Config{PubSubSystem: "aws", AWSAccountID: "000000000000", AWSRegion: "us-east-1"},
		setup: func(t *testing.T) (func(), message.Publisher, message.Subscriber) {
			t.Helper()
			origLoader := AWSDefaultConfigLoader
			origTopic := SNSTopicResolverFactory
			origPub := SNSPublisherFactory
			origSub := SNSSubscriberFactory
			pub := &testPublisher{}
			sub := &testSubscriber{}
			AWSDefaultConfigLoader = func(ctx context.Context, optFns ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
				return aws.Config{Region: "us-east-1"}, nil
			}
			SNSTopicResolverFactory = func(accountID, region string) (*sns.GenerateArnTopicResolver, error) {
				return origTopic(accountID, region)
			}
			SNSPublisherFactory = func(cfg sns.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
				if cfg.TopicResolver == nil {
					t.Fatal("expected topic resolver to be set")
				}
				return pub, nil
			}
			SNSSubscriberFactory = func(cfg sns.SubscriberConfig, sqsCfg sqs.SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {
				return sub, nil
			}
			return func() {
				AWSDefaultConfigLoader = origLoader
				SNSTopicResolverFactory = origTopic
				SNSPublisherFactory = origPub
				SNSSubscriberFactory = origSub
			}, pub, sub
		},
	}
}

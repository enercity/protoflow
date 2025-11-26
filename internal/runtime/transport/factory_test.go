package transport

import (
	"context"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-aws/sns"
	"github.com/ThreeDotsLabs/watermill-aws/sqs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"github.com/enercity/protoflow/internal/runtime/config"
)

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

	for _, tc := range []factoryBuildCase{awsFactoryCase(), channelFactoryCase()} {
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

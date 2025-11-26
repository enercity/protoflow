package transport

import (
	"context"
	"errors"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-aws/sns"
	"github.com/ThreeDotsLabs/watermill-aws/sqs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	amazonsns "github.com/aws/aws-sdk-go-v2/service/sns"
	amazonsqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/enercity/protoflow/internal/runtime/config"
)

func TestCreateAWSConfigSetsRegion(t *testing.T) {
	origLoader := AWSDefaultConfigLoader
	t.Cleanup(func() { AWSDefaultConfigLoader = origLoader })

	AWSDefaultConfigLoader = func(ctx context.Context, optFns ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{Region: "us-east-1"}, nil
	}

	conf := &config.Config{AWSRegion: "ap-southeast-2"}
	cfg, err := createAWSConfig(context.Background(), conf, watermill.NopLogger{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Region != "ap-southeast-2" {
		t.Fatalf("expected region override, got %s", cfg.Region)
	}
}

func TestCreateAWSConfigReturnsError(t *testing.T) {
	origLoader := AWSDefaultConfigLoader
	t.Cleanup(func() { AWSDefaultConfigLoader = origLoader })

	AWSDefaultConfigLoader = func(ctx context.Context, optFns ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{}, errors.New("boom")
	}

	if _, err := createAWSConfig(context.Background(), &config.Config{}, watermill.NopLogger{}); err == nil {
		t.Fatal("expected error when config loader fails")
	}
}

func TestCreateAWSConfigReturnsErrorWithRegion(t *testing.T) {
	origLoader := AWSDefaultConfigLoader
	t.Cleanup(func() { AWSDefaultConfigLoader = origLoader })

	AWSDefaultConfigLoader = func(ctx context.Context, optFns ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{}, errors.New("boom")
	}

	conf := &config.Config{AWSRegion: "us-east-1"}
	if _, err := createAWSConfig(context.Background(), conf, watermill.NopLogger{}); err == nil {
		t.Fatal("expected error when config loader fails")
	}
}

func TestCreateAwsPublisherAccountFallbacks(t *testing.T) {
	origTopic := SNSTopicResolverFactory
	origPub := SNSPublisherFactory
	t.Cleanup(func() {
		SNSTopicResolverFactory = origTopic
		SNSPublisherFactory = origPub
	})

	recordedAccountIDs := make([]string, 0, 2)
	SNSTopicResolverFactory = func(accountID, region string) (*sns.GenerateArnTopicResolver, error) {
		recordedAccountIDs = append(recordedAccountIDs, accountID)
		return origTopic(accountID, region)
	}

	pub := &testPublisher{}
	SNSPublisherFactory = func(cfg sns.PublisherConfig, _ watermill.LoggerAdapter) (message.Publisher, error) {
		for _, opt := range cfg.OptFns {
			opt(&amazonsns.Options{})
		}
		return pub, nil
	}

	conf := &config.Config{AWSAccountID: " '123456789012' ", AWSRegion: "eu-central-1"}
	logger := watermill.NopLogger{}
	cfg := &aws.Config{}

	publisher, err := createAwsPublisher(conf, logger, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if publisher != pub {
		t.Fatal("publisher not returned")
	}

	conf.AWSEndpoint = "http://localhost:4566"
	conf.AWSAccountID = "bad"
	if _, err := createAwsPublisher(conf, logger, cfg); err != nil {
		t.Fatalf("unexpected error when falling back: %v", err)
	}

	if len(recordedAccountIDs) != 2 {
		t.Fatalf("expected two resolver invocations, got %d", len(recordedAccountIDs))
	}
	if recordedAccountIDs[0] != "123456789012" {
		t.Fatalf("expected trimmed account id, got %s", recordedAccountIDs[0])
	}
	if recordedAccountIDs[1] != "000000000000" {
		t.Fatalf("expected fallback account id, got %s", recordedAccountIDs[1])
	}
}

func TestCreateAwsPublisherInvalidEndpoint(t *testing.T) {
	conf := &config.Config{AWSAccountID: "123456789012", AWSEndpoint: "://bad"}
	if _, err := createAwsPublisher(conf, watermill.NopLogger{}, &aws.Config{}); err == nil {
		t.Fatal("expected error for invalid endpoint")
	}
}

func TestCreateAwsSubscriberFallbacks(t *testing.T) {
	origTopic := SNSTopicResolverFactory
	origSub := SNSSubscriberFactory
	t.Cleanup(func() {
		SNSTopicResolverFactory = origTopic
		SNSSubscriberFactory = origSub
	})

	recordedAccountIDs := make([]string, 0, 2)
	SNSTopicResolverFactory = func(accountID, region string) (*sns.GenerateArnTopicResolver, error) {
		recordedAccountIDs = append(recordedAccountIDs, accountID)
		return origTopic(accountID, region)
	}

	sub := &testSubscriber{}
	SNSSubscriberFactory = func(cfg sns.SubscriberConfig, sqsCfg sqs.SubscriberConfig, _ watermill.LoggerAdapter) (message.Subscriber, error) {
		for _, opt := range cfg.OptFns {
			opt(&amazonsns.Options{})
		}
		for _, opt := range sqsCfg.OptFns {
			opt(&amazonsqs.Options{})
		}
		return sub, nil
	}

	conf := &config.Config{AWSAccountID: "", AWSRegion: "us-west-2", AWSEndpoint: "http://localhost:4566"}
	cfg := &aws.Config{BaseEndpoint: aws.String("http://localhost:4566")}
	result, err := createAwsSubscriber(conf, watermill.NopLogger{}, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != sub {
		t.Fatal("subscriber not returned")
	}
	if recordedAccountIDs[0] != "000000000000" {
		t.Fatalf("expected default account id, got %s", recordedAccountIDs[0])
	}
}

func TestCreateAwsSubscriberRegionPropagation(t *testing.T) {
	origTopic := SNSTopicResolverFactory
	origSub := SNSSubscriberFactory
	t.Cleanup(func() {
		SNSTopicResolverFactory = origTopic
		SNSSubscriberFactory = origSub
	})

	SNSTopicResolverFactory = func(accountID, region string) (*sns.GenerateArnTopicResolver, error) {
		return origTopic(accountID, region)
	}

	var capturedSNSConfig sns.SubscriberConfig
	var capturedSQSConfig sqs.SubscriberConfig
	SNSSubscriberFactory = func(cfg sns.SubscriberConfig, sqsCfg sqs.SubscriberConfig, _ watermill.LoggerAdapter) (message.Subscriber, error) {
		capturedSNSConfig = cfg
		capturedSQSConfig = sqsCfg
		return &testSubscriber{}, nil
	}

	testRegion := "eu-west-1"
	conf := &config.Config{AWSAccountID: "123456789012", AWSRegion: testRegion}
	cfg := &aws.Config{Region: testRegion}

	_, err := createAwsSubscriber(conf, watermill.NopLogger{}, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify that the SNS subscriber config has the correct region from the AWS config
	if capturedSNSConfig.AWSConfig.Region != testRegion {
		t.Fatalf("expected SNS subscriber AWSConfig.Region to be %q, got %q", testRegion, capturedSNSConfig.AWSConfig.Region)
	}

	// Verify that the SQS subscriber config also has the correct region
	if capturedSQSConfig.AWSConfig.Region != testRegion {
		t.Fatalf("expected SQS subscriber AWSConfig.Region to be %q, got %q", testRegion, capturedSQSConfig.AWSConfig.Region)
	}
}

func TestCreateAwsSubscriberInvalidBaseEndpoint(t *testing.T) {
	conf := &config.Config{}
	cfg := &aws.Config{BaseEndpoint: aws.String("://bad")}
	if _, err := createAwsSubscriber(conf, watermill.NopLogger{}, cfg); err == nil {
		t.Fatal("expected error for invalid base endpoint")
	}
}

func TestCreateAwsSubscriberResolverError(t *testing.T) {
	origTopic := SNSTopicResolverFactory
	t.Cleanup(func() { SNSTopicResolverFactory = origTopic })

	SNSTopicResolverFactory = func(accountID, region string) (*sns.GenerateArnTopicResolver, error) {
		return nil, errors.New("resolve")
	}

	if _, err := createAwsSubscriber(&config.Config{}, watermill.NopLogger{}, &aws.Config{}); err == nil {
		t.Fatal("expected error on resolver failure")
	}
}

func TestCreateAwsPublisherResolverError(t *testing.T) {
	origTopic := SNSTopicResolverFactory
	t.Cleanup(func() { SNSTopicResolverFactory = origTopic })

	SNSTopicResolverFactory = func(accountID, region string) (*sns.GenerateArnTopicResolver, error) {
		return nil, errors.New("resolver")
	}

	if _, err := createAwsPublisher(&config.Config{AWSAccountID: "123456789012"}, watermill.NopLogger{}, &aws.Config{}); err == nil {
		t.Fatal("expected error when resolver fails")
	}
}

func TestCreateAwsPublisherFactoryError(t *testing.T) {
	origPub := SNSPublisherFactory
	t.Cleanup(func() { SNSPublisherFactory = origPub })

	SNSPublisherFactory = func(cfg sns.PublisherConfig, _ watermill.LoggerAdapter) (message.Publisher, error) {
		return nil, errors.New("publish")
	}

	if _, err := createAwsPublisher(&config.Config{AWSAccountID: "123456789012"}, watermill.NopLogger{}, &aws.Config{}); err == nil {
		t.Fatal("expected error when publisher creation fails")
	}
}

func TestCreateAwsSubscriberFactoryError(t *testing.T) {
	origSub := SNSSubscriberFactory
	t.Cleanup(func() { SNSSubscriberFactory = origSub })

	SNSSubscriberFactory = func(cfg sns.SubscriberConfig, sqsCfg sqs.SubscriberConfig, _ watermill.LoggerAdapter) (message.Subscriber, error) {
		return nil, errors.New("sub")
	}

	if _, err := createAwsSubscriber(&config.Config{}, watermill.NopLogger{}, &aws.Config{}); err == nil {
		t.Fatal("expected error when subscriber creation fails")
	}
}

func TestResolveAccountAndRegionFallsBackToAWSConfigRegion(t *testing.T) {
	conf := &config.Config{}
	account, region := resolveAccountAndRegion(conf, watermill.NopLogger{}, "env-region")
	if account != "" {
		t.Fatalf("expected empty account, got %s", account)
	}
	if region != "env-region" {
		t.Fatalf("expected fallback region to be used, got %s", region)
	}

	conf.AWSRegion = "explicit"
	_, region = resolveAccountAndRegion(conf, watermill.NopLogger{}, "env-region")
	if region != "explicit" {
		t.Fatalf("expected explicit config region, got %s", region)
	}
}

func TestResolveAccountAndRegionInvalidID(t *testing.T) {
	conf := &config.Config{
		AWSAccountID: "invalid",
		AWSEndpoint:  "http://localhost:4566", // Localstack
	}

	// We need to access resolveAccountAndRegion which is private.
	// But we can test it via createAwsSubscriber or createAwsPublisher.
	// Or just call it if we are in the same package.
	// aws_test.go is package transport.

	id, _ := resolveAccountAndRegion(conf, watermill.NopLogger{}, "us-east-1")
	if id != localstackAccountID {
		t.Fatalf("expected localstack account ID, got %s", id)
	}
}

func TestAddEndpointResolver(t *testing.T) {
	aCfg := &aws.Config{}
	conf := &config.Config{}
	snsOpts, sqsOpts, err := addEndpointResolver(conf, aCfg, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(snsOpts) != 0 || len(sqsOpts) != 0 {
		t.Fatalf("expected no options when base endpoint unset")
	}

	endpoint := "http://localhost:4566"
	aCfg.BaseEndpoint = aws.String(endpoint)
	conf.AWSEndpoint = endpoint
	snsOpts, sqsOpts, err = addEndpointResolver(conf, aCfg, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(snsOpts) != 1 || len(sqsOpts) != 1 {
		t.Fatalf("expected endpoint overrides to be configured")
	}

	aCfg.BaseEndpoint = aws.String("http://invalid-url" + string(byte(0x7f)))
	if _, _, err = addEndpointResolver(conf, aCfg, nil, nil); err == nil {
		t.Fatal("expected error for invalid endpoint")
	}
}

func TestSafeAWSRegion(t *testing.T) {
	if safeAWSRegion(nil) != "" {
		t.Fatal("expected empty string for nil config")
	}
	cfg := &aws.Config{Region: "us-west-2"}
	if safeAWSRegion(cfg) != "us-west-2" {
		t.Fatal("expected region from config")
	}
}

func TestCreateAwsSubscriberFails(t *testing.T) {
	origFactory := SNSSubscriberFactory
	defer func() { SNSSubscriberFactory = origFactory }()

	SNSSubscriberFactory = func(cfg sns.SubscriberConfig, sqsCfg sqs.SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {
		return nil, errors.New("subscriber fail")
	}

	_, err := createAwsSubscriber(&config.Config{}, watermill.NopLogger{}, &aws.Config{})
	if err == nil {
		t.Fatal("expected error when subscriber factory fails")
	}
}

func TestCreateAwsPublisherFails(t *testing.T) {
	origFactory := SNSPublisherFactory
	defer func() { SNSPublisherFactory = origFactory }()

	SNSPublisherFactory = func(cfg sns.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
		return nil, errors.New("publisher fail")
	}

	_, err := createAwsPublisher(&config.Config{}, watermill.NopLogger{}, &aws.Config{})
	if err == nil {
		t.Fatal("expected error when publisher factory fails")
	}
}

func TestCreateTopicResolverFails(t *testing.T) {
	origFactory := SNSTopicResolverFactory
	defer func() { SNSTopicResolverFactory = origFactory }()

	SNSTopicResolverFactory = func(accountID, region string) (*sns.GenerateArnTopicResolver, error) {
		return nil, errors.New("resolver fail")
	}

	_, err := createAwsSubscriber(&config.Config{}, watermill.NopLogger{}, &aws.Config{})
	if err == nil {
		t.Fatal("expected error when topic resolver factory fails")
	}
}

func TestAwsTransportSuccess(t *testing.T) {
	// Mock everything to succeed
	origLoader := AWSDefaultConfigLoader
	defer func() { AWSDefaultConfigLoader = origLoader }()
	AWSDefaultConfigLoader = func(ctx context.Context, optFns ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{Region: "us-east-1"}, nil
	}

	origPub := SNSPublisherFactory
	defer func() { SNSPublisherFactory = origPub }()
	SNSPublisherFactory = func(cfg sns.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
		return &testPublisher{}, nil
	}

	origSub := SNSSubscriberFactory
	defer func() { SNSSubscriberFactory = origSub }()
	SNSSubscriberFactory = func(cfg sns.SubscriberConfig, sqsCfg sqs.SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {
		return &testSubscriber{}, nil
	}

	_, err := awsTransport(context.Background(), &config.Config{AWSAccountID: "123456789012"}, watermill.NopLogger{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAwsTransportFailsOnInvalidEndpoint(t *testing.T) {
	// Mock config loader to succeed
	origLoader := AWSDefaultConfigLoader
	defer func() { AWSDefaultConfigLoader = origLoader }()
	AWSDefaultConfigLoader = func(ctx context.Context, optFns ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{Region: "us-east-1"}, nil
	}

	// Invalid endpoint URL
	conf := &config.Config{
		AWSEndpoint: ":invalid-url",
	}

	_, err := awsTransport(context.Background(), conf, watermill.NopLogger{})
	if err == nil {
		t.Fatal("expected error when endpoint is invalid")
	}
}

func TestAwsTransportFailsOnSubscriberError(t *testing.T) {
	// Mock config loader to succeed
	origLoader := AWSDefaultConfigLoader
	defer func() { AWSDefaultConfigLoader = origLoader }()
	AWSDefaultConfigLoader = func(ctx context.Context, optFns ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{Region: "us-east-1"}, nil
	}

	// Mock publisher to succeed
	origPub := SNSPublisherFactory
	defer func() { SNSPublisherFactory = origPub }()
	SNSPublisherFactory = func(cfg sns.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
		return &testPublisher{}, nil
	}

	// Mock subscriber to fail
	origSub := SNSSubscriberFactory
	defer func() { SNSSubscriberFactory = origSub }()
	SNSSubscriberFactory = func(cfg sns.SubscriberConfig, sqsCfg sqs.SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {
		return nil, errors.New("subscriber fail")
	}

	_, err := awsTransport(context.Background(), &config.Config{AWSAccountID: "123"}, watermill.NopLogger{})
	if err == nil {
		t.Fatal("expected error when subscriber factory fails")
	}
}

func TestAwsSubscriberQueueNameGenerator(t *testing.T) {
	// Mock factory to capture config
	origSub := SNSSubscriberFactory
	defer func() { SNSSubscriberFactory = origSub }()

	var capturedCfg sns.SubscriberConfig
	var capturedSqsCfg sqs.SubscriberConfig
	SNSSubscriberFactory = func(cfg sns.SubscriberConfig, sqsCfg sqs.SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {
		capturedCfg = cfg
		capturedSqsCfg = sqsCfg
		return &testSubscriber{}, nil
	}

	// Mock other factories to succeed
	origLoader := AWSDefaultConfigLoader
	defer func() { AWSDefaultConfigLoader = origLoader }()
	AWSDefaultConfigLoader = func(ctx context.Context, optFns ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{Region: "us-east-1"}, nil
	}
	origPub := SNSPublisherFactory
	defer func() { SNSPublisherFactory = origPub }()
	SNSPublisherFactory = func(cfg sns.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
		return &testPublisher{}, nil
	}

	_, err := awsTransport(context.Background(), &config.Config{AWSAccountID: "123"}, watermill.NopLogger{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify region is properly propagated to SNS subscriber config
	if capturedCfg.AWSConfig.Region != "us-east-1" {
		t.Fatalf("expected SNS subscriber config region 'us-east-1', got '%s'", capturedCfg.AWSConfig.Region)
	}

	// Verify region is properly propagated to SQS subscriber config
	if capturedSqsCfg.AWSConfig.Region != "us-east-1" {
		t.Fatalf("expected SQS subscriber config region 'us-east-1', got '%s'", capturedSqsCfg.AWSConfig.Region)
	}

	if capturedCfg.GenerateSqsQueueName == nil {
		t.Fatal("expected GenerateSqsQueueName to be set")
	}

	// Test the generator
	name, err := capturedCfg.GenerateSqsQueueName(context.Background(), "arn:aws:sns:us-east-1:123:topic")
	if err != nil {
		t.Fatalf("unexpected error generating queue name: %v", err)
	}
	if name != "topic-subscriber" {
		t.Fatalf("expected queue name 'topic-subscriber', got '%s'", name)
	}

	// Test generator error
	_, err = capturedCfg.GenerateSqsQueueName(context.Background(), "invalid-arn")
	if err == nil {
		t.Fatal("expected error for invalid ARN")
	}
}

func TestResolveAccountAndRegionNilConfig(t *testing.T) {
	account, region := resolveAccountAndRegion(nil, watermill.NopLogger{}, "fallback")
	if account != "" {
		t.Fatalf("expected empty account for nil config, got %s", account)
	}
	if region != "fallback" {
		t.Fatalf("expected fallback region for nil config, got %s", region)
	}
}

func TestCreateAWSConfigNilConfig(t *testing.T) {
	cfg, err := createAWSConfig(context.Background(), nil, watermill.NopLogger{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected config to be returned")
	}
}

func TestStaticCredentialsProvider(t *testing.T) {
	provider := staticCredentialsProvider("key", "secret")
	creds, err := provider.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if creds.AccessKeyID != "key" {
		t.Errorf("expected key, got %s", creds.AccessKeyID)
	}
	if creds.SecretAccessKey != "secret" {
		t.Errorf("expected secret, got %s", creds.SecretAccessKey)
	}
}

func TestAwsTransportFailures(t *testing.T) {
	// 1. createAWSConfig fails
	origLoader := AWSDefaultConfigLoader
	t.Cleanup(func() { AWSDefaultConfigLoader = origLoader })
	AWSDefaultConfigLoader = func(ctx context.Context, optFns ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{}, errors.New("config fail")
	}
	if _, err := awsTransport(context.Background(), &config.Config{}, watermill.NopLogger{}); err == nil {
		t.Fatal("expected error when config fails")
	}

	// Reset loader
	AWSDefaultConfigLoader = origLoader

	// 2. createAwsPublisher fails
	origPub := SNSPublisherFactory
	t.Cleanup(func() { SNSPublisherFactory = origPub })
	SNSPublisherFactory = func(cfg sns.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
		return nil, errors.New("pub fail")
	}
	if _, err := awsTransport(context.Background(), &config.Config{}, watermill.NopLogger{}); err == nil {
		t.Fatal("expected error when publisher fails")
	}
	SNSPublisherFactory = origPub

	// 3. createAwsSubscriber fails
	origSub := SNSSubscriberFactory
	t.Cleanup(func() { SNSSubscriberFactory = origSub })
	SNSSubscriberFactory = func(cfg sns.SubscriberConfig, sqsCfg sqs.SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {
		return nil, errors.New("sub fail")
	}
	if _, err := awsTransport(context.Background(), &config.Config{}, watermill.NopLogger{}); err == nil {
		t.Fatal("expected error when subscriber fails")
	}
}

func TestCreateAWSConfigWithCredentials(t *testing.T) {
	conf := &config.Config{
		AWSAccessKeyID:     "key",
		AWSSecretAccessKey: "secret",
	}
	// Just ensure it doesn't panic and returns config
	cfg, err := createAWSConfig(context.Background(), conf, watermill.NopLogger{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected config")
	}
}

type testPublisher struct{}

func (p *testPublisher) Publish(topic string, messages ...*message.Message) error { return nil }

func (p *testPublisher) Close() error { return nil }

type testSubscriber struct{}

func (s *testSubscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	ch := make(chan *message.Message)
	close(ch)
	return ch, nil
}

func (s *testSubscriber) Close() error { return nil }

func TestCreateAwsSubscriber_EndpointError(t *testing.T) {
	cfg := &aws.Config{
		BaseEndpoint: aws.String("http://localhost:4566"),
	}
	conf := &config.Config{AWSEndpoint: ":/invalid-url"}

	_, err := createAwsSubscriber(conf, watermill.NopLogger{}, cfg)
	if err == nil {
		t.Fatal("expected endpoint error")
	}
}

func TestMakeSqsQueueNameGenerator(t *testing.T) {
	gen := makeSqsQueueNameGenerator("sub")

	// Valid ARN
	name, err := gen(context.Background(), "arn:aws:sns:us-east-1:123456789012:MyTopic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if name != "MyTopic-sub" {
		t.Fatalf("expected MyTopic-sub, got %s", name)
	}

	// Invalid ARN
	_, err = gen(context.Background(), "invalid")
	if err == nil {
		t.Fatal("expected error for invalid ARN")
	}
}

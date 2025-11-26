package transport

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-aws/sns"
	"github.com/ThreeDotsLabs/watermill-aws/sqs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	amazonsns "github.com/aws/aws-sdk-go-v2/service/sns"
	amazonsqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	transport "github.com/aws/smithy-go/endpoints"

	"github.com/enercity/protoflow/internal/runtime/config"
)

var (
	AWSDefaultConfigLoader  = awsconfig.LoadDefaultConfig
	SNSTopicResolverFactory = sns.NewGenerateArnTopicResolver
	SNSPublisherFactory     = func(cfg sns.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
		return sns.NewPublisher(cfg, logger)
	}
	SNSSubscriberFactory = func(cfg sns.SubscriberConfig, sqsCfg sqs.SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {
		return sns.NewSubscriber(cfg, sqsCfg, logger)
	}
)

const (
	localstackAccountID = "000000000000"
	awsAccountIDLength  = 12
)

func awsTransport(ctx context.Context, conf *config.Config, logger watermill.LoggerAdapter) (Transport, error) {
	cfg, err := createAWSConfig(ctx, conf, logger)
	if err != nil {
		return Transport{}, err
	}
	logger.Info("Created AWS config", watermill.LogFields{
		"region":          safeAWSRegion(cfg),
		"custom_endpoint": hasCustomEndpoint(cfg),
	})

	publisher, err := createAwsPublisher(conf, logger, cfg)
	if err != nil {
		return Transport{}, err
	}
	subscriber, err := createAwsSubscriber(conf, logger, cfg)
	if err != nil {
		return Transport{}, err
	}
	return Transport{Publisher: publisher, Subscriber: subscriber}, nil
}

func createAWSConfig(ctx context.Context, conf *config.Config, logger watermill.LoggerAdapter) (*aws.Config, error) {
	var opts []func(*awsconfig.LoadOptions) error

	if conf != nil {
		if conf.AWSRegion != "" {
			logger.Info("Setting AWS region from config", watermill.LogFields{"region": conf.AWSRegion})
			opts = append(opts, awsconfig.WithRegion(conf.AWSRegion))
		}
		if conf.AWSAccessKeyID != "" && conf.AWSSecretAccessKey != "" {
			logger.Info("Using static AWS credentials from config", watermill.LogFields{})
			opts = append(opts, awsconfig.WithCredentialsProvider(staticCredentialsProvider(conf.AWSAccessKeyID, conf.AWSSecretAccessKey)))
		}
	}

	cfg, err := AWSDefaultConfigLoader(ctx, opts...)
	if err != nil {
		fields := watermill.LogFields{}
		if conf != nil && conf.AWSRegion != "" {
			fields["requested_region"] = conf.AWSRegion
		}
		logger.Error("Failed to load AWS default config", err, fields)
		return nil, err
	}
	// Ensure region is set even if the loader ignores options (e.g. in tests)
	if conf != nil && conf.AWSRegion != "" {
		cfg.Region = conf.AWSRegion
	}

	return &cfg, nil
}

func createAwsPublisher(conf *config.Config, logger watermill.LoggerAdapter, cfg *aws.Config) (message.Publisher, error) {
	accountID, region := resolveAccountAndRegion(conf, logger, safeAWSRegion(cfg))
	logger.Info("Create AWS Publisher",
		watermill.LogFields{
			"accountID": accountID,
			"region":    region,
		})

	topicResolver, err := createTopicResolver(accountID, region, logger)
	if err != nil {
		return nil, err
	}
	publisherConfig, err := buildPublisherConfig(conf, cfg, topicResolver, logger)
	if err != nil {
		return nil, err
	}
	return SNSPublisherFactory(publisherConfig, logger)
}

func makeSqsQueueNameGenerator() func(context.Context, sns.TopicArn) (string, error) {
	return func(ctx context.Context, snsTopic sns.TopicArn) (string, error) {
		topic, err := sns.ExtractTopicNameFromTopicArn(snsTopic)
		if err != nil {
			return "", err
		}
		return string(topic), nil
	}
}

func createAwsSubscriber(conf *config.Config, logger watermill.LoggerAdapter, cfg *aws.Config) (message.Subscriber, error) {
	accountID, region := resolveAccountAndRegion(conf, logger, safeAWSRegion(cfg))
	topicResolver, err := createTopicResolver(accountID, region, logger)
	if err != nil {
		return nil, err
	}

	var snsOpts []func(*amazonsns.Options)
	var sqsOpts []func(*amazonsqs.Options)
	snsOpts, sqsOpts, err = addEndpointResolver(conf, cfg, snsOpts, sqsOpts)
	if err != nil {
		return nil, err
	}

	// AWS resources (SNS topics, SQS queues, subscriptions, policies) must be
	// provisioned via Terraform/IaC. Protoflow never creates them at runtime.
	subscriberConfig := sns.SubscriberConfig{
		AWSConfig:                  *cfg,
		OptFns:                     snsOpts,
		TopicResolver:              topicResolver,
		GenerateSqsQueueName:       makeSqsQueueNameGenerator(),
		DoNotCreateSqsSubscription: true,
		DoNotSetQueueAccessPolicy:  true,
	}

	return SNSSubscriberFactory(
		subscriberConfig,
		sqs.SubscriberConfig{
			AWSConfig:                   *cfg,
			OptFns:                      sqsOpts,
			DoNotCreateQueueIfNotExists: true,
		},
		logger,
	)
}

func addEndpointResolver(conf *config.Config, cfg *aws.Config, snsOpts []func(*amazonsns.Options), sqsOpts []func(*amazonsqs.Options)) ([]func(*amazonsns.Options), []func(*amazonsqs.Options), error) {
	if hasCustomEndpoint(cfg) {
		url, err := awsEndpointURL(conf)
		if err != nil {
			return nil, nil, err
		}
		parsedURL, err := url.Parse(*cfg.BaseEndpoint)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse BaseEndpoint: %w", err)
		}
		snsOpts = []func(*amazonsns.Options){
			amazonsns.WithEndpointResolverV2(sns.OverrideEndpointResolver{
				Endpoint: transport.Endpoint{
					URI: *parsedURL,
				},
			}),
		}
		sqsOpts = []func(*amazonsqs.Options){
			amazonsqs.WithEndpointResolverV2(sqs.OverrideEndpointResolver{
				Endpoint: transport.Endpoint{
					URI: *parsedURL,
				},
			}),
		}
	}
	return snsOpts, sqsOpts, nil
}

func resolveAccountAndRegion(conf *config.Config, logger watermill.LoggerAdapter, fallbackRegion string) (string, string) {
	if conf == nil {
		return "", fallbackRegion
	}

	accountID := strings.Trim(conf.AWSAccountID, "\"' ")
	region := conf.AWSRegion
	if region == "" {
		region = fallbackRegion
	}

	if accountID == "" && useLocalstackEndpoint(conf) {
		accountID = localstackAccountID
		logger.Info("AWS account ID empty; using LocalStack default", watermill.LogFields{"accountID": accountID})
		return accountID, region
	}

	if accountID != "" && len(accountID) != awsAccountIDLength && useLocalstackEndpoint(conf) {
		logger.Info("Invalid AWS account ID; falling back to LocalStack default", watermill.LogFields{"accountID": accountID})
		accountID = localstackAccountID
	}

	return accountID, region
}

func useLocalstackEndpoint(conf *config.Config) bool {
	return conf != nil && conf.AWSEndpoint != ""
}

func createTopicResolver(accountID, region string, logger watermill.LoggerAdapter) (sns.TopicResolver, error) {
	topicResolver, err := SNSTopicResolverFactory(accountID, region)
	if err != nil {
		logger.Error("Failed to create SNS topic resolver", err, watermill.LogFields{
			"accountID": accountID,
			"region":    region,
		})
		return nil, err
	}

	return topicResolver, nil
}

func buildPublisherConfig(conf *config.Config, cfg *aws.Config, topicResolver sns.TopicResolver, logger watermill.LoggerAdapter) (sns.PublisherConfig, error) {
	endpoint, err := awsEndpointURL(conf)
	if err != nil {
		logger.Error("Failed to parse AWS endpoint", err, watermill.LogFields{"endpoint": conf.AWSEndpoint})
		return sns.PublisherConfig{}, err
	}

	// AWS resources (SNS topics) must be provisioned via Terraform/IaC.
	// Protoflow never creates them at runtime.
	publisherConfig := sns.PublisherConfig{
		TopicResolver:               topicResolver,
		AWSConfig:                   *cfg,
		Marshaler:                   sns.DefaultMarshalerUnmarshaler{},
		DoNotCreateTopicIfNotExists: true,
	}

	if endpoint != nil {
		endpointStr := endpoint.String()
		publisherConfig.OptFns = []func(*amazonsns.Options){
			func(o *amazonsns.Options) {
				o.BaseEndpoint = aws.String(endpointStr)
			},
		}
	}

	return publisherConfig, nil
}

func awsEndpointURL(conf *config.Config) (*url.URL, error) {
	if conf == nil || conf.AWSEndpoint == "" {
		return nil, nil
	}

	parsedURL, err := url.Parse(conf.AWSEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to parse AWS endpoint: %w", err)
	}

	return parsedURL, nil
}

func safeAWSRegion(cfg *aws.Config) string {
	if cfg == nil {
		return ""
	}
	return cfg.Region
}

func hasCustomEndpoint(cfg *aws.Config) bool {
	return cfg != nil && cfg.BaseEndpoint != nil && *cfg.BaseEndpoint != ""
}

func staticCredentialsProvider(accessKeyID, secretAccessKey string) aws.CredentialsProvider {
	return aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		return aws.Credentials{
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
		}, nil
	})
}

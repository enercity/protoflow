package config

import (
	"fmt"
	"time"
)

// Config groups the Pub/Sub settings required to initialise the Service. Each
// transport only uses the keys that are relevant to it.
type Config struct {
	// PubSubSystem selects the backing message infrastructure. Supported values:
	// "aws" (SNS/SQS) or "channel" (in-memory).
	PubSubSystem string

	// PoisonQueue receives messages that cannot be processed even after retries.
	PoisonQueue string

	// AWS (SNS/SQS) configuration.
	AWSRegion          string
	AWSAccountID       string
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	// AWSEndpoint optionally points to a custom endpoint (for example, LocalStack
	// in local development).
	AWSEndpoint string

	// RetryMiddleware tuning. Zero values fall back to library defaults.
	RetryMaxRetries      int
	RetryInitialInterval time.Duration
	RetryMaxInterval     time.Duration

	// Metrics configuration.
	MetricsEnabled bool
	// MetricsPort is the port where Prometheus metrics will be exposed.
	MetricsPort int

	// WebUI configuration.
	WebUIEnabled bool
	// WebUIPort is the port where the WebUI API will be exposed. Defaults to 8081.
	WebUIPort int
}

func (c Config) String() string {
	// Create a copy to avoid modifying the original
	copy := c
	if copy.AWSSecretAccessKey != "" {
		copy.AWSSecretAccessKey = "***REDACTED***"
	}
	if copy.AWSAccessKeyID != "" {
		copy.AWSAccessKeyID = "***REDACTED***"
	}
	// Use a type alias to avoid infinite recursion when printing
	type configAlias Config
	return fmt.Sprintf("%+v", configAlias(copy))
}

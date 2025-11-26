# Configuration Guide

Protoflow is designed to be flexible. Centralize your broker wiring, middleware stack, and custom dependencies behind `protoflow.Config` and `protoflow.ServiceDependencies`.

## Transport Selection

Set `Config.PubSubSystem` to one of the built-in transports and configure the relevant fields.

### AWS SNS/SQS (`"aws"`)

| Field | Purpose |
| :--- | :--- |
| `AWSRegion` | AWS Region for clients |
| `AWSAccountID` | Account ID for ARNs (default: `000000000000` for LocalStack) |
| `AWSEndpoint` | Override endpoint (useful for LocalStack) |
| `AWSAccessKeyID` | Explicit credentials (optional) |
| `AWSSecretAccessKey` | Explicit credentials (optional) |

> **Note:** Protoflow expects all AWS resources (SNS topics, SQS queues, SNS→SQS subscriptions, and queue policies) to be provisioned via Terraform or other IaC tools. It will **not** create them at runtime.

### Go Channel (`"channel"`)

| Field | Purpose |
| :--- | :--- |
| N/A | In-memory transport, no configuration needed. Useful for testing or simple local applications. |

## Common Configuration

| Field | Description |
| :--- | :--- |
| `PoisonQueue` | The "dead letter" queue for failed messages |
| `RetryMaxRetries` | Max attempts before giving up (default: exponential backoff) |
| `RetryInitialInterval` | Delay before the first retry |
| `RetryMaxInterval` | Cap on the retry backoff duration |

*Tip: Leave zero values to use Protoflow's defaults.*

## Middleware & Dependencies

`protoflow.ServiceDependencies` is the injection point for custom behavior.

- **`Validator`** (`ProtoValidator`): Plug in your own validation logic (runs in `ProtoValidateMiddleware`).
- **`Outbox`** (`OutboxStore`): Persist events before they are published.
- **`Middlewares`** (`[]MiddlewareRegistration`): Add your own middleware to the stack (prepended or appended).
- **`DisableDefaultMiddlewares`**: Set to `true` to manually register only what you need.

Default middleware order:

1. Correlation ID injector
2. Message logger
3. Proto validation
4. Outbox persistence
5. OpenTelemetry tracing
6. Retry with exponential backoff
7. Poison queue forwarder
8. Panic recoverer

## Custom Transports

`ServiceDependencies.TransportFactory` replaces the built-in transport selection. The factory receives the context, resolved config, and the Watermill logger adapter. Return a `protoflow.Transport` with both publisher and subscriber instances.

```go
type gcppubsubFactory struct{ client *pubsub.Client }

func (f gcppubsubFactory) Build(ctx context.Context, conf *protoflow.Config, logger watermill.LoggerAdapter) (protoflow.Transport, error) {
    pub := newPubSubPublisher(f.client, logger)
    sub := newPubSubSubscriber(f.client, conf, logger)
    return protoflow.Transport{Publisher: pub, Subscriber: sub}, nil
}

svc := protoflow.NewService(cfg, logger, ctx,
    protoflow.ServiceDependencies{TransportFactory: gcppubsubFactory{client: client}},
)
```

This pattern allows you to integrate brokers such as GCP Pub/Sub, Redis Streams, or other transports without modifying Protoflow.

# Protoflow

[![Go Reference](https://pkg.go.dev/badge/github.com/enercity/protoflow.svg)](https://pkg.go.dev/github.com/enercity/protoflow)
[![Go Report Card](https://goreportcard.com/badge/github.com/enercity/protoflow)](https://goreportcard.com/report/github.com/enercity/protoflow)
[![CI](https://github.com/enercity/protoflow/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/enercity/protoflow/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/DrBlury/protoflow/branch/main/graph/badge.svg)](https://codecov.io/gh/DrBlury/protoflow)
[![Latest Tag](https://img.shields.io/github/v/tag/DrBlury/protoflow?sort=semver&label=latest%20tag)](https://github.com/enercity/protoflow/tags)
[![License](https://img.shields.io/github/license/DrBlury/protoflow)](LICENSE)

**Stop writing plumbing. Start shipping features.**

Protoflow is a productivity layer for [Watermill](https://watermill.io/) that simplifies event-driven architecture. It manages routers, publishers, subscribers, and middleware so you can focus on your domain logic.

Whether you are using Protobufs or JSON, Protoflow provides a type-safe, production-ready foundation for Kafka, RabbitMQ, AWS SNS/SQS, NATS, and Go Channels.

## Feature Highlights

- **Type-Safe Handlers**: Generic `RegisterProtoHandler` and `RegisterJSONHandler` helpers keep your code clean.
- **Instant Wiring**: Switch between Kafka, RabbitMQ, AWS SNS/SQS, NATS, and Go Channels with a single config change.
- **Batteries Included**: A robust default middleware stack with correlation IDs, structured logging, validation, outbox pattern, OpenTelemetry tracing, retries, poison queues, and panic recovery.
- **Developer Experience**: Built for clarity and ease of use. Extension points exist where you need them, but the defaults just work.
- **Safe Publishing**: Emit events from anywhere in your application with confidence using our publishing helpers.

## Quick Start

1. **Install**: `go get github.com/enercity/protoflow` (Go 1.25+).
2. **Configure**: Set up `protoflow.Config`.
3. **Launch**: Create a `Service`, register your handlers, and `Start`.

```go
// 1. Configure your transport (Kafka, RabbitMQ, AWS, NATS, Channel, etc.)
cfg := &protoflow.Config{
    PubSubSystem: "channel", // Use in-memory channel for simple pub/sub
    PoisonQueue:  "orders.poison",
}

// 2. Use your preferred logger (slog, logrus, zap, etc.)
logger := protoflow.NewSlogServiceLogger(slog.Default())
svc := protoflow.NewService(cfg, logger, ctx, protoflow.ServiceDependencies{})

// 3. Register a strongly-typed handler
must(protoflow.RegisterProtoHandler(svc, protoflow.ProtoHandlerRegistration[*models.OrderCreated]{
    Name:         "orders-created",
    ConsumeQueue: "orders.created",
    Handler: func(ctx context.Context, evt protoflow.ProtoMessageContext[*models.OrderCreated]) ([]protoflow.ProtoMessageOutput, error) {
        // Your business logic goes here!
        evt.Logger.Info("Order received", protoflow.LogFields{"id": evt.Payload.OrderId})
        return nil, nil
    },
}))

// 4. Start the service
go func() { _ = svc.Start(ctx) }()
```

Want to emit events? Need metadata handling? Check out the [Handlers Guide](docs/handlers/README.md).

## Logging

`protoflow.ServiceLogger` unifies logging across the router, middleware, and transports. Just wrap your favorite logger and pass it to `NewService`:

- **`protoflow.NewSlogServiceLogger`**: Adapts `log/slog` (standard library).
- **`protoflow.NewEntryServiceLogger`**: Adapts any structured logger (logrus, zerolog, etc.) that fits the `EntryLoggerAdapter[T]` interface.

```go
// Use your existing logger instance
svc := protoflow.NewService(cfg,
    protoflow.NewEntryServiceLogger(myFancyLogger),
    ctx,
    protoflow.ServiceDependencies{},
)
```

We handle the translation to Watermill's logger interface.

## Documentation

- [**Handlers Guide**](docs/handlers/README.md): Master typed handlers, metadata, and publishing.
- [**Configuration Guide**](docs/configuration/README.md): Configure transports, middleware, and dependency injection.
- [**Development Guide**](docs/development/README.md): Guide to local development, testing, and the Taskfile workflow.

## Examples

Check out `examples/` for runnable code:

- `simple`: A basic example of Protoflow.
- `json`: Typed JSON handlers with metadata enrichment.
- `proto`: Protobuf handlers with generated models.
- `full`: A comprehensive example with custom middleware, validators, outbox, and more.

Run them with: `go run ./examples/<name>`

## Development Workflow

We use `task` to manage development commands.

- `task lint`: Keep the code sparkling.
- `task test`: Run the gauntlet.

See the [Development Guide](docs/development/README.md) for the full menu.

Run the full test suite with `go test ./...` (or `task test`) before sending changes.

## Contribution guidelines

1. Fork the repo and branch from `main`.
2. Run `task lint` and `task test` (or the equivalent commands) before opening a PR.
3. Add or update docs inside `docs/` and package comments when you ship new features.
4. Keep commits focused; attach context in PR descriptions so reviewers understand the transport, middleware, or handler surface you touched.

## License

Protoflow is available under the [MIT License](LICENSE).

package runtime

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ThreeDotsLabs/watermill/components/metrics"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/encoding/protojson"

	idspkg "github.com/enercity/protoflow/internal/runtime/ids"
	loggingpkg "github.com/enercity/protoflow/internal/runtime/logging"
)

// MiddlewareBuilder constructs a handler middleware using the provided service instance.
type MiddlewareBuilder func(*Service) (message.HandlerMiddleware, error)

// MiddlewareRegistration captures how a middleware should be registered on a Service router.
type MiddlewareRegistration struct {
	Name       string
	Middleware message.HandlerMiddleware
	Builder    MiddlewareBuilder
}

// RetryMiddlewareConfig customises the retry middleware behaviour.
type RetryMiddlewareConfig struct {
	MaxRetries      int
	InitialInterval time.Duration
	MaxInterval     time.Duration
	RetryIf         func(error) bool
}

func (cfg RetryMiddlewareConfig) withDefaults() RetryMiddlewareConfig {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 5
	}
	if cfg.InitialInterval <= 0 {
		cfg.InitialInterval = time.Second
	}
	if cfg.MaxInterval <= 0 {
		cfg.MaxInterval = 16 * time.Second
	}
	return cfg
}

// DefaultMiddlewares returns the standard middleware chain used by the Service constructor.
func DefaultMiddlewares() []MiddlewareRegistration {
	return []MiddlewareRegistration{
		CorrelationIDMiddleware(),
		LogMessagesMiddleware(nil),
		ProtoValidateMiddleware(),
		OutboxMiddleware(),
		TracerMiddleware(),
		MetricsMiddleware(),
		RetryMiddleware(RetryMiddlewareConfig{}),
		PoisonQueueMiddleware(nil),
		RecovererMiddleware(),
	}
}

// MetricsMiddleware adds Prometheus metrics to the handler.
func MetricsMiddleware() MiddlewareRegistration {
	return MiddlewareRegistration{
		Name: "metrics",
		Builder: func(s *Service) (message.HandlerMiddleware, error) {
			if !s.Conf.MetricsEnabled {
				return nil, nil
			}

			metricsBuilder := metrics.NewPrometheusMetricsBuilder(
				prometheus.DefaultRegisterer,
				"protoflow",
				s.Conf.PubSubSystem,
			)

			metricsBuilder.AddPrometheusRouterMetrics(s.router)

			if s.Conf.MetricsPort > 0 {
				s.RegisterHTTPHandler(s.Conf.MetricsPort, "/metrics", promhttp.Handler())
			}

			return metricsBuilder.NewRouterMiddleware().Middleware, nil
		},
	}
}

// CorrelationIDMiddleware ensures each processed message carries a correlation identifier.
func CorrelationIDMiddleware() MiddlewareRegistration {
	return MiddlewareRegistration{
		Name: "correlation_id",
		Builder: func(s *Service) (message.HandlerMiddleware, error) {
			return s.correlationIDMiddleware(), nil
		},
	}
}

// LogMessagesMiddleware logs the full payload and metadata of handled messages.
func LogMessagesMiddleware(logger loggingpkg.ServiceLogger) MiddlewareRegistration {
	return MiddlewareRegistration{
		Name: "log_messages",
		Builder: func(s *Service) (message.HandlerMiddleware, error) {
			l := logger
			if l == nil {
				l = s.Logger
			}
			if l == nil {
				return nil, errors.New("log messages middleware requires a logger")
			}
			return s.logMessagesMiddleware(l), nil
		},
	}
}

// ProtoValidateMiddleware unmarshals and validates protobuf payloads when possible.
func ProtoValidateMiddleware() MiddlewareRegistration {
	return MiddlewareRegistration{
		Name: "proto_validate",
		Builder: func(s *Service) (message.HandlerMiddleware, error) {
			return s.protoValidateMiddleware(), nil
		},
	}
}

// OutboxMiddleware persists outgoing messages when an OutboxStore is configured.
func OutboxMiddleware() MiddlewareRegistration {
	return MiddlewareRegistration{
		Name: "outbox",
		Builder: func(s *Service) (message.HandlerMiddleware, error) {
			return s.outboxMiddleware(), nil
		},
	}
}

// TracerMiddleware wraps handler execution in an OpenTelemetry span.
func TracerMiddleware() MiddlewareRegistration {
	return MiddlewareRegistration{
		Name: "tracer",
		Builder: func(s *Service) (message.HandlerMiddleware, error) {
			return s.tracerMiddleware(), nil
		},
	}
}

// RetryMiddleware retries handler execution using the provided configuration (defaults applied to zero values).
func RetryMiddleware(cfg RetryMiddlewareConfig) MiddlewareRegistration {
	normalized := cfg.withDefaults()
	return MiddlewareRegistration{
		Name: "retry",
		Builder: func(s *Service) (message.HandlerMiddleware, error) {
			return s.retryMiddlewareWithConfig(normalized), nil
		},
	}
}

// PoisonQueueMiddleware publishes messages that match the supplied filter to the configured poison queue.
func PoisonQueueMiddleware(filter func(error) bool) MiddlewareRegistration {
	return MiddlewareRegistration{
		Name: "poison_queue",
		Builder: func(s *Service) (message.HandlerMiddleware, error) {
			f := filter
			if f == nil {
				f = func(err error) bool {
					_, ok := err.(*UnprocessableEventError)
					return ok
				}
			}
			return s.poisonMiddlewareWithFilter(f)
		},
	}
}

// RecovererMiddleware converts panics into handler errors so they can be retried or sent to the poison queue.
func RecovererMiddleware() MiddlewareRegistration {
	return MiddlewareRegistration{
		Name:       "recoverer",
		Middleware: middleware.Recoverer,
	}
}

// RegisterMiddleware attaches the supplied middleware to the router.
func (s *Service) RegisterMiddleware(cfg MiddlewareRegistration) error {
	if s.router == nil {
		return errors.New("router is not initialised")
	}

	var mw message.HandlerMiddleware
	switch {
	case cfg.Middleware != nil:
		mw = cfg.Middleware
	case cfg.Builder != nil:
		var err error
		mw, err = cfg.Builder(s)
		if err != nil {
			return err
		}
	default:
		return errors.New("middleware registration requires Middleware or Builder")
	}

	if mw == nil {
		return nil
	}

	s.router.AddMiddleware(mw)
	return nil
}

// correlationIDMiddleware injects a correlation ID into the message metadata when missing.
func (s *Service) correlationIDMiddleware() message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			if _, ok := msg.Metadata["correlation_id"]; !ok {
				msg.Metadata["correlation_id"] = idspkg.CreateULID()
			}
			return h(msg)
		}
	}
}

// protoValidateMiddleware validates protobuf payloads using the registered message prototypes.
func (s *Service) protoValidateMiddleware() message.HandlerMiddleware {
	if s.validator == nil {
		return func(h message.HandlerFunc) message.HandlerFunc {
			return h
		}
	}

	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			eventType, ok := msg.Metadata["event_message_schema"]
			if !ok {
				slog.Warn("missing event_message_schema in metadata")
				return h(msg)
			}

			s.protoRegistryMu.RLock()
			newProtoFunc, ok := s.protoRegistry[eventType]
			s.protoRegistryMu.RUnlock()
			if !ok {
				slog.Error("unknown event type", "event_message_schema", eventType)
				return nil, &UnprocessableEventError{
					eventMessage: string(msg.Payload),
					err:          fmt.Errorf("unknown event type: %s", eventType),
				}
			}

			protoMsg := newProtoFunc()
			if err := protojson.Unmarshal(msg.Payload, protoMsg); err != nil {
				slog.Error("failed to unmarshal protobuf message", "error", err, "event_message_schema", eventType)
				return nil, &UnprocessableEventError{
					eventMessage: string(msg.Payload),
					err:          err,
				}
			}
			if err := s.validator.Validate(protoMsg); err != nil {
				slog.Error("failed to validate protobuf message", "error", err, "event_message_schema", eventType)
				return nil, &UnprocessableEventError{
					eventMessage: string(msg.Payload),
					err:          err,
				}
			}
			return h(msg)
		}
	}
}

// poisonMiddlewareWithFilter publishes poison messages based on the provided filter.
func (s *Service) poisonMiddlewareWithFilter(filter func(err error) bool) (message.HandlerMiddleware, error) {
	if s.Conf == nil {
		return nil, errors.New("service config is required for poison queue middleware")
	}
	if s.publisher == nil {
		return nil, errors.New("publisher is required for poison queue middleware")
	}

	mw, err := middleware.PoisonQueueWithFilter(
		s.publisher,
		s.Conf.PoisonQueue,
		filter,
	)
	if err != nil {
		return nil, err
	}

	return mw, nil
}

// logMessagesMiddleware logs all processed messages with their metadata.
func (s *Service) logMessagesMiddleware(logger loggingpkg.ServiceLogger) message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			logger.Debug("Processing message", loggingpkg.LogFields{
				"message_uuid": msg.UUID,
				"payload":      string(msg.Payload),
				"metadata":     msg.Metadata,
			})
			return h(msg)
		}
	}
}

// outboxMiddleware stores outgoing messages in the configured OutboxStore, if present.
func (s *Service) outboxMiddleware() message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			if s.outbox == nil {
				return h(msg)
			}

			outgoingMessages, err := h(msg)
			if err != nil {
				return nil, err
			}

			if len(outgoingMessages) == 0 {
				return outgoingMessages, nil
			}

			for _, outMsg := range outgoingMessages {
				eventType := outMsg.Metadata["event_message_schema"]
				if eventType == "" {
					eventType = "unknown_event"
				}
				if err := s.outbox.StoreOutgoingMessage(msg.Context(), eventType, outMsg.UUID, string(outMsg.Payload)); err != nil {
					return nil, err
				}
			}

			return outgoingMessages, nil
		}
	}
}

// retryMiddleware retries message processing with exponential backoff.
func (s *Service) retryMiddleware() message.HandlerMiddleware {
	return s.retryMiddlewareWithConfig(RetryMiddlewareConfig{})
}

func (s *Service) retryMiddlewareWithConfig(cfg RetryMiddlewareConfig) message.HandlerMiddleware {
	normalized := cfg.withDefaults()
	return middleware.Retry{
		MaxRetries:      normalized.MaxRetries,
		InitialInterval: normalized.InitialInterval,
		MaxInterval:     normalized.MaxInterval,
		ShouldRetry: func(params middleware.RetryParams) bool {
			if normalized.RetryIf != nil {
				return normalized.RetryIf(params.Err)
			}
			return true
		},
	}.Middleware
}

// tracerMiddleware wraps message handling with an OpenTelemetry span.
func (s *Service) tracerMiddleware() message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			tracer := otel.Tracer("events-service-tracer")
			ctx, span := tracer.Start(
				msg.Context(),
				"ProcessMessage",
			)
			defer span.End()
			msg.SetContext(ctx)

			span.SetAttributes(
				attribute.String("message.uuid", msg.UUID),
				attribute.String("message.metadata", fmt.Sprintf("%v", msg.Metadata)),
			)
			return h(msg)
		}
	}
}

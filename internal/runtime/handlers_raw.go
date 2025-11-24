package runtime

import (
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"google.golang.org/protobuf/proto"

	errspkg "github.com/enercity/protoflow/internal/runtime/errors"
)

type handlerRegistration struct {
	Name               string
	ConsumeQueue       string
	Subscriber         message.Subscriber
	PublishQueue       string
	Publisher          message.Publisher
	Handler            message.HandlerFunc
	consumeMessageType proto.Message
}

// MessageHandlerRegistration wires a raw Watermill handler without typed helpers.
type MessageHandlerRegistration struct {
	Name         string
	ConsumeQueue string
	PublishQueue string
	Handler      message.HandlerFunc
	Subscriber   message.Subscriber
	Publisher    message.Publisher
}

// RegisterMessageHandler attaches the provided handler to the service router.
func RegisterMessageHandler(svc *Service, cfg MessageHandlerRegistration) error {
	if svc == nil {
		return errspkg.ErrServiceRequired
	}

	return svc.registerHandler(handlerRegistration{
		Name:         cfg.Name,
		ConsumeQueue: cfg.ConsumeQueue,
		PublishQueue: cfg.PublishQueue,
		Subscriber:   cfg.Subscriber,
		Publisher:    cfg.Publisher,
		Handler:      cfg.Handler,
	})
}

func (s *Service) registerHandler(cfg handlerRegistration) error {
	if cfg.Handler == nil {
		return errspkg.ErrHandlerRequired
	}
	if cfg.ConsumeQueue == "" {
		return errspkg.ErrConsumeQueueRequired
	}
	if cfg.Subscriber == nil {
		cfg.Subscriber = s.subscriber
	}
	if cfg.Publisher == nil {
		cfg.Publisher = s.publisher
	}
	if cfg.consumeMessageType != nil {
		s.registerProtoType(cfg.consumeMessageType)
		if cfg.Name == "" {
			cfg.Name = fmt.Sprintf("%T-Handler", cfg.consumeMessageType)
		}
	}
	if cfg.Name == "" {
		return errspkg.ErrHandlerNameRequired
	}

	stats := newHandlerStats(cfg.Name, cfg.ConsumeQueue, cfg.PublishQueue, s.getResourceTracker())
	info := &HandlerInfo{
		Name:         cfg.Name,
		ConsumeQueue: cfg.ConsumeQueue,
		PublishQueue: cfg.PublishQueue,
		Stats:        stats,
	}

	s.handlersMu.Lock()
	s.handlers = append(s.handlers, info)
	s.handlersMu.Unlock()

	cfg.Handler = wrapHandlerWithStats(cfg.Handler, stats, s.getErrorClassifier())

	s.router.AddHandler(
		cfg.Name,
		cfg.ConsumeQueue,
		cfg.Subscriber,
		cfg.PublishQueue,
		cfg.Publisher,
		cfg.Handler,
	)

	return nil
}

// RegisterProtoMessage exposes a proto message type for validation without registering a handler.
func (s *Service) RegisterProtoMessage(msg proto.Message) {
	s.registerProtoType(msg)
}

func (s *Service) registerProtoType(msg proto.Message) {
	if msg == nil {
		return
	}

	typeName := fmt.Sprintf("%T", msg)

	s.protoRegistryMu.Lock()
	s.protoRegistry[typeName] = func() proto.Message {
		return msg.ProtoReflect().New().Interface()
	}
	s.protoRegistryMu.Unlock()
}

func wrapHandlerWithStats(handler message.HandlerFunc, stats *HandlerStats, classifier ErrorClassifier) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		invocation := stats.onMessageStart(msg)
		start := time.Now()
		msgs, err := handler(msg)
		duration := time.Since(start)

		stats.onMessageFinish(invocation, duration, err, classifier)

		return msgs, err
	}
}

package runtime

import (
	errspkg "github.com/enercity/protoflow/internal/runtime/errors"
	handlerpkg "github.com/enercity/protoflow/internal/runtime/handlers"
	"google.golang.org/protobuf/proto"
)

// RegisterProtoHandler converts the typed handler into a Watermill handler and registers it on the Service router.
func RegisterProtoHandler[T proto.Message](svc *Service, cfg handlerpkg.ProtoHandlerRegistration[T]) error {
	if svc == nil {
		return errspkg.ErrServiceRequired
	}

	var zero T
	prototype, err := handlerpkg.EnsureProtoPrototype(zero)
	if err != nil {
		return err
	}

	resolvedOpts := handlerpkg.ApplyProtoHandlerOptions(cfg.Options)

	var validate func(proto.Message) error
	if cfg.ValidateOutgoing && svc.validator != nil {
		validate = func(msg proto.Message) error {
			return svc.validator.Validate(msg)
		}
	}

	wrapped, err := handlerpkg.BuildProtoHandler(prototype, cfg.Handler, validate, NewMessageFromProto, svc.Logger)
	if err != nil {
		return err
	}

	if err := svc.registerHandler(handlerRegistration{
		Name:               cfg.Name,
		ConsumeQueue:       cfg.ConsumeQueue,
		PublishQueue:       cfg.PublishQueue,
		Handler:            wrapped,
		consumeMessageType: prototype,
	}); err != nil {
		return err
	}

	for _, emitted := range resolvedOpts.AdditionalPublishTypes {
		svc.registerProtoType(emitted)
	}

	return nil
}

package runtime

import (
	errspkg "github.com/enercity/protoflow/internal/runtime/errors"
	handlerpkg "github.com/enercity/protoflow/internal/runtime/handlers"
)

// RegisterJSONHandler converts the typed JSON handler into a Watermill handler and registers it.
func RegisterJSONHandler[T any, O any](svc *Service, cfg handlerpkg.JSONHandlerRegistration[T, O]) error {
	if svc == nil {
		return errspkg.ErrServiceRequired
	}

	wrapped, err := handlerpkg.BuildJSONHandler(cfg.Handler, svc.Logger)
	if err != nil {
		return err
	}

	return svc.registerHandler(handlerRegistration{
		Name:         cfg.Name,
		ConsumeQueue: cfg.ConsumeQueue,
		PublishQueue: cfg.PublishQueue,
		Handler:      wrapped,
	})
}

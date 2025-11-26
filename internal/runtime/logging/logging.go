package logging

import (
	"context"
	"log/slog"

	"github.com/ThreeDotsLabs/watermill"
)

// LogFields represents structured logging key/value pairs used by Protoflow.
type LogFields map[string]any

// ServiceLogger is the minimal logging contract required by Protoflow services.
// It maps directly onto Watermill's logging needs so applications can adapt
// their existing loggers without depending on slog.
type ServiceLogger interface {
	With(fields LogFields) ServiceLogger
	Debug(ctx context.Context, msg string, fields LogFields)
	Info(ctx context.Context, msg string, fields LogFields)
	Error(ctx context.Context, msg string, err error, fields LogFields)
	Trace(ctx context.Context, msg string, fields LogFields)
}

// EntryLogger represents the legacy non-generic entry adapter constraint. It
// remains exported so existing code that referenced protoflow.EntryLogger keeps
// compiling, but NewEntryServiceLogger now works with any logger that satisfies
// EntryLoggerAdapter[T].
type EntryLogger interface {
	EntryLoggerAdapter[EntryLogger]
}

// EntryLoggerAdapter captures the capabilities required by
// NewEntryServiceLogger. The constraint is generic so third-party entry-like
// loggers (for example, loggers whose methods return their own concrete
// interface type) can be used without additional wrappers.
// Compatible with github.com/enercity/lib-logger/v4.Entry interface.
type EntryLoggerAdapter[T any] interface {
	Error(ctx context.Context, args ...any)
	Info(ctx context.Context, args ...any)
	Debug(ctx context.Context, args ...any)
	Trace(ctx context.Context, args ...any)
	WithError(err error) T
	WithField(key string, value any) T
}

var logLevelMapping = map[slog.Level]slog.Level{
	slog.LevelDebug: slog.LevelDebug,
	slog.LevelInfo:  slog.LevelInfo,
	slog.LevelWarn:  slog.LevelWarn,
	slog.LevelError: slog.LevelError,
}

// NewSlogServiceLogger wraps a slog.Logger so it satisfies the ServiceLogger
// interface. This matches the previous behaviour of Protoflow services.
func NewSlogServiceLogger(log *slog.Logger) ServiceLogger {
	if log == nil {
		panic("protoflow: slog logger cannot be nil")
	}
	return NewWatermillServiceLogger(watermill.NewSlogLoggerWithLevelMapping(log, logLevelMapping))
}

// NewWatermillServiceLogger wraps an existing Watermill LoggerAdapter so it can
// be supplied to NewService.
func NewWatermillServiceLogger(logger watermill.LoggerAdapter) ServiceLogger {
	if logger == nil {
		panic("protoflow: watermill logger cannot be nil")
	}
	return &watermillServiceLogger{inner: logger}
}

// NewEntryServiceLogger wraps an EntryLogger (for example a logrus.Entry) so it
// can be consumed by Protoflow services without forcing additional logging
// adapters.
func NewEntryServiceLogger[T EntryLoggerAdapter[T]](entry T) ServiceLogger {
	if any(entry) == nil {
		panic("protoflow: entry logger cannot be nil")
	}
	return &entryServiceLogger[T]{entry: entry}
}

type watermillServiceLogger struct {
	inner watermill.LoggerAdapter
}

type entryServiceLogger[T EntryLoggerAdapter[T]] struct {
	entry T
}

func (e *entryServiceLogger[T]) With(fields LogFields) ServiceLogger {
	if len(fields) == 0 {
		return e
	}
	return &entryServiceLogger[T]{entry: applyEntryFields(e.entry, fields)}
}

func (e *entryServiceLogger[T]) Debug(ctx context.Context, msg string, fields LogFields) {
	applyEntryFields(e.entry, fields).Debug(ctx, msg)
}

func (e *entryServiceLogger[T]) Info(ctx context.Context, msg string, fields LogFields) {
	applyEntryFields(e.entry, fields).Info(ctx, msg)
}

func (e *entryServiceLogger[T]) Error(ctx context.Context, msg string, err error, fields LogFields) {
	logger := applyEntryFields(e.entry, fields)
	if err != nil {
		logger = logger.WithError(err)
	}
	logger.Error(ctx, msg)
}

func (e *entryServiceLogger[T]) Trace(ctx context.Context, msg string, fields LogFields) {
	applyEntryFields(e.entry, fields).Trace(ctx, msg)
}

func (w *watermillServiceLogger) With(fields LogFields) ServiceLogger {
	return &watermillServiceLogger{inner: w.inner.With(toWatermillFields(fields))}
}

func (w *watermillServiceLogger) Debug(_ context.Context, msg string, fields LogFields) {
	w.inner.Debug(msg, toWatermillFields(fields))
}

func (w *watermillServiceLogger) Info(_ context.Context, msg string, fields LogFields) {
	w.inner.Info(msg, toWatermillFields(fields))
}

func (w *watermillServiceLogger) Error(_ context.Context, msg string, err error, fields LogFields) {
	w.inner.Error(msg, err, toWatermillFields(fields))
}

func (w *watermillServiceLogger) Trace(_ context.Context, msg string, fields LogFields) {
	w.inner.Trace(msg, toWatermillFields(fields))
}

type serviceLoggerAdapter struct {
	base ServiceLogger
}

// NewWatermillAdapter converts a ServiceLogger into a Watermill LoggerAdapter so
// internal runtime components can reuse the same logger abstraction.
func NewWatermillAdapter(log ServiceLogger) watermill.LoggerAdapter {
	if log == nil {
		panic("protoflow: ServiceLogger cannot be nil")
	}
	return &serviceLoggerAdapter{base: log}
}

func (s *serviceLoggerAdapter) Error(msg string, err error, fields watermill.LogFields) {
	s.base.Error(context.Background(), msg, err, fromWatermillFields(fields))
}

func (s *serviceLoggerAdapter) Info(msg string, fields watermill.LogFields) {
	s.base.Info(context.Background(), msg, fromWatermillFields(fields))
}

func (s *serviceLoggerAdapter) Debug(msg string, fields watermill.LogFields) {
	s.base.Debug(context.Background(), msg, fromWatermillFields(fields))
}

func (s *serviceLoggerAdapter) Trace(msg string, fields watermill.LogFields) {
	s.base.Trace(context.Background(), msg, fromWatermillFields(fields))
}

func (s *serviceLoggerAdapter) With(fields watermill.LogFields) watermill.LoggerAdapter {
	return &serviceLoggerAdapter{base: s.base.With(fromWatermillFields(fields))}
}

func toWatermillFields(fields LogFields) watermill.LogFields {
	if len(fields) == 0 {
		return nil
	}
	return watermill.LogFields(fields)
}

func fromWatermillFields(fields watermill.LogFields) LogFields {
	if len(fields) == 0 {
		return nil
	}
	return LogFields(fields)
}

func applyEntryFields[T EntryLoggerAdapter[T]](entry T, fields LogFields) T {
	if len(fields) == 0 || any(entry) == nil {
		return entry
	}
	enriched := entry
	for key, value := range fields {
		enriched = enriched.WithField(key, value)
	}
	return enriched
}

package logging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
)

func TestEntryServiceLoggerDelegates(t *testing.T) {
	entry := newFakeEntry()
	logger := NewEntryServiceLogger(entry)

	ctx := context.Background()
	logger.Info(ctx, "boot", LogFields{"system": "test"})

	child := logger.With(LogFields{"base": "value"})
	child.Debug(ctx, "child", LogFields{"child": "value"})

	boom := errors.New("boom")
	child.Error(ctx, "child failed", boom, LogFields{"child": "value"})

	child.Trace(ctx, "trace", nil)

	logs := entry.recorder.logs
	if len(logs) != 4 {
		t.Fatalf("expected 4 log entries, got %d", len(logs))
	}

	if logs[0].level != "info" || logs[0].msg != "boot" {
		t.Fatalf("unexpected first log: %#v", logs[0])
	}
	if got := logs[0].fields["system"]; got != "test" {
		t.Fatalf("missing system field, got %v", got)
	}

	if logs[1].level != "debug" {
		t.Fatalf("expected debug level on second log, got %s", logs[1].level)
	}
	if logs[1].fields["base"] != "value" || logs[1].fields["child"] != "value" {
		t.Fatalf("expected merged fields on second log, got %#v", logs[1].fields)
	}

	if logs[2].level != "error" || logs[2].err != boom {
		t.Fatalf("expected error with boom, got %#v", logs[2])
	}

	if logs[3].level != "trace" {
		t.Fatalf("expected trace level on final log, got %s", logs[3].level)
	}
}

func TestEntryServiceLoggerPanicsOnNil(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when entry logger nil")
		}
	}()
	NewEntryServiceLogger[EntryLogger](nil)
}

func TestEntryServiceLoggerWithNilFields(t *testing.T) {
	entry := newFakeEntry()
	logger := NewEntryServiceLogger(entry)
	child := logger.With(nil)
	child.Info(context.Background(), "test", nil)

	if len(entry.recorder.logs) != 1 {
		t.Fatalf("expected 1 log entry, got %d", len(entry.recorder.logs))
	}
}

func TestWatermillServiceLoggerDelegates(t *testing.T) {
	base := newRecordingWatermillLogger()
	logger := NewWatermillServiceLogger(base)

	ctx := context.Background()
	logger.Debug(ctx, "dbg", LogFields{"component": "watermill"})
	logger.Info(ctx, "info", nil)
	logger.Trace(ctx, "trace", LogFields{"trace": true})
	logger.Error(ctx, "oops", errors.New("boom"), LogFields{"failed": true})

	child := logger.With(LogFields{"child": "yes"})
	typedChild, ok := child.(*watermillServiceLogger)
	if !ok {
		t.Fatal("expected watermill service logger")
	}
	typedChild.Info(ctx, "child_info", nil)

	if len(base.entries) != 6 {
		t.Fatalf("expected 6 log entries, got %d", len(base.entries))
	}
	if base.entries[0].level != "debug" || base.entries[0].fields["component"] != "watermill" {
		t.Fatalf("unexpected first entry: %#v", base.entries[0])
	}
	if base.entries[4].fields["child"] != "yes" {
		t.Fatalf("expected With to propagate fields, got %#v", base.entries[4].fields)
	}
}

func TestWatermillServiceLoggerPanicsOnNilLogger(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when logger nil")
		}
	}()
	NewWatermillServiceLogger(nil)
}

func TestSlogLoggerPanicsOnNil(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when slog logger nil")
		}
	}()
	NewSlogServiceLogger(nil)
}

func TestWatermillAdapterDelegates(t *testing.T) {
	base := &recordingServiceLogger{}
	adapter := NewWatermillAdapter(base)

	adapter.Debug("dbg", watermill.LogFields{"k": "v"})
	adapter.Info("info", nil)
	adapter.Trace("trace", nil)
	adapter.Error("err", errors.New("boom"), nil)

	child := adapter.With(watermill.LogFields{"child": "yes"})
	typedChild, ok := child.(*serviceLoggerAdapter)
	if !ok {
		t.Fatal("expected service logger adapter child")
	}
	childBase, ok := typedChild.base.(*recordingServiceLogger)
	if !ok {
		t.Fatal("expected recording service logger child base")
	}
	child.Info("child_info", nil)

	if len(base.entries) != 4 {
		t.Fatalf("expected 4 delegated entries on base, got %d", len(base.entries))
	}
	if len(childBase.entries) != 2 {
		t.Fatalf("expected child logger to record entries, got %d", len(childBase.entries))
	}
	if childBase.entries[0].fields["child"] != "yes" {
		t.Fatalf("expected child fields to be preserved")
	}
}

func TestWatermillAdapterPanicsOnNil(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when adapter nil")
		}
	}()
	NewWatermillAdapter(nil)
}

func TestApplyEntryFieldsIgnoresNil(t *testing.T) {
	entry := newFakeEntry()
	enriched := applyEntryFields(entry, nil)
	if enriched != entry {
		t.Fatal("expected nil fields to return same instance")
	}
	withFields := applyEntryFields(entry, LogFields{"k": "v"})
	if withFields == entry {
		t.Fatal("expected new entry when fields provided")
	}
}

func TestWatermillFieldConversions(t *testing.T) {
	if toWatermillFields(nil) != nil {
		t.Fatal("expected nil conversion to return nil")
	}
	if fromWatermillFields(nil) != nil {
		t.Fatal("expected nil conversion to return nil")
	}

	wm := toWatermillFields(LogFields{"a": 1})
	if wm["a"].(int) != 1 {
		t.Fatalf("unexpected watermill fields: %#v", wm)
	}
	lf := fromWatermillFields(wm)
	if lf["a"].(int) != 1 {
		t.Fatalf("unexpected log fields: %#v", lf)
	}
}

func TestNewSlogServiceLoggerWrapsSlog(t *testing.T) {
	base := slog.New(slog.NewTextHandler(testWriter{}, nil))
	logger := NewSlogServiceLogger(base)
	logger.Info(context.Background(), "hello", LogFields{"k": "v"})
}

type recordingWatermillLogger struct {
	entries []watermillEntry
	sink    *[]watermillEntry
}

func newRecordingWatermillLogger() *recordingWatermillLogger {
	logger := &recordingWatermillLogger{}
	logger.sink = &logger.entries
	return logger
}

func (r *recordingWatermillLogger) record(entry watermillEntry) {
	if r.sink == nil {
		r.sink = &r.entries
	}
	*r.sink = append(*r.sink, entry)
}

type watermillEntry struct {
	level  string
	fields watermill.LogFields
	err    error
}

func (r *recordingWatermillLogger) Error(msg string, err error, fields watermill.LogFields) {
	r.record(watermillEntry{level: "error", fields: fields, err: err})
}

func (r *recordingWatermillLogger) Info(msg string, fields watermill.LogFields) {
	r.record(watermillEntry{level: "info", fields: fields})
}

func (r *recordingWatermillLogger) Debug(msg string, fields watermill.LogFields) {
	r.record(watermillEntry{level: "debug", fields: fields})
}

func (r *recordingWatermillLogger) Trace(msg string, fields watermill.LogFields) {
	r.record(watermillEntry{level: "trace", fields: fields})
}

func (r *recordingWatermillLogger) With(fields watermill.LogFields) watermill.LoggerAdapter {
	child := newRecordingWatermillLogger()
	child.sink = r.sink
	child.record(watermillEntry{level: "with", fields: fields})
	return child
}

type recordingServiceLogger struct {
	entries []loggedEntry
}

type loggedEntry struct {
	level  string
	msg    string
	fields LogFields
	err    error
}

func (r *recordingServiceLogger) With(fields LogFields) ServiceLogger {
	cloned := &recordingServiceLogger{}
	cloned.entries = append(cloned.entries, loggedEntry{level: "with", fields: fields})
	return cloned
}

func (r *recordingServiceLogger) Debug(_ context.Context, msg string, fields LogFields) {
	r.entries = append(r.entries, loggedEntry{level: "debug", msg: msg, fields: fields})
}

func (r *recordingServiceLogger) Info(_ context.Context, msg string, fields LogFields) {
	r.entries = append(r.entries, loggedEntry{level: "info", msg: msg, fields: fields})
}

func (r *recordingServiceLogger) Error(_ context.Context, msg string, err error, fields LogFields) {
	r.entries = append(r.entries, loggedEntry{level: "error", msg: msg, fields: fields, err: err})
}

func (r *recordingServiceLogger) Trace(_ context.Context, msg string, fields LogFields) {
	r.entries = append(r.entries, loggedEntry{level: "trace", msg: msg, fields: fields})
}

type fakeEntry struct {
	recorder *entryRecorder
	fields   LogFields
	err      error
}

type entryRecorder struct {
	logs []loggedEntry
}

func newFakeEntry() *fakeEntry {
	return &fakeEntry{recorder: &entryRecorder{}}
}

func (f *fakeEntry) clone() *fakeEntry {
	clonedFields := cloneFields(f.fields)
	return &fakeEntry{recorder: f.recorder, fields: clonedFields, err: f.err}
}

func (f *fakeEntry) Error(_ context.Context, args ...any) {
	f.append("error", args...)
}

func (f *fakeEntry) Info(_ context.Context, args ...any) {
	f.append("info", args...)
}

func (f *fakeEntry) Debug(_ context.Context, args ...any) {
	f.append("debug", args...)
}

func (f *fakeEntry) Trace(_ context.Context, args ...any) {
	f.append("trace", args...)
}

func (f *fakeEntry) WithError(err error) *fakeEntry {
	clone := f.clone()
	clone.err = err
	return clone
}

func (f *fakeEntry) WithField(key string, value any) *fakeEntry {
	clone := f.clone()
	if clone.fields == nil {
		clone.fields = make(LogFields)
	}
	clone.fields[key] = value
	return clone
}

func (f *fakeEntry) append(level string, args ...any) {
	msg := fmt.Sprint(args...)
	entry := loggedEntry{
		level:  level,
		msg:    msg,
		fields: cloneFields(f.fields),
		err:    f.err,
	}
	f.recorder.logs = append(f.recorder.logs, entry)
}

func cloneFields(fields LogFields) LogFields {
	if len(fields) == 0 {
		return nil
	}
	out := make(LogFields, len(fields))
	for k, v := range fields {
		out[k] = v
	}
	return out
}

type testWriter struct{}

func (testWriter) Write(p []byte) (int, error) { return len(p), nil }

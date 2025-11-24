package runtime

import (
	"errors"
	"fmt"
	"testing"

	loggingpkg "github.com/enercity/protoflow/internal/runtime/logging"
)

func TestEntryServiceLogger(t *testing.T) {
	entry := newFakeEntry()
	logger := loggingpkg.NewEntryServiceLogger(entry)

	logger.Info("boot", loggingpkg.LogFields{"system": "test"})

	child := logger.With(loggingpkg.LogFields{"base": "value"})
	child.Debug("child", loggingpkg.LogFields{"child": "value"})

	boom := errors.New("boom")
	child.Error("child failed", boom, loggingpkg.LogFields{"child": "value"})

	child.Trace("trace", nil)

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

type fakeEntry struct {
	recorder *entryRecorder
	fields   loggingpkg.LogFields
	err      error
}

type entryRecorder struct {
	logs []loggedEntry
}

type loggedEntry struct {
	level  string
	msg    string
	fields loggingpkg.LogFields
	err    error
}

func newFakeEntry() *fakeEntry {
	return &fakeEntry{recorder: &entryRecorder{}}
}

func (f *fakeEntry) clone() *fakeEntry {
	clonedFields := cloneFields(f.fields)
	return &fakeEntry{recorder: f.recorder, fields: clonedFields, err: f.err}
}

func (f *fakeEntry) Error(args ...any) {
	f.append("error", args...)
}

func (f *fakeEntry) Info(args ...any) {
	f.append("info", args...)
}

func (f *fakeEntry) Debug(args ...any) {
	f.append("debug", args...)
}

func (f *fakeEntry) Trace(args ...any) {
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
		clone.fields = make(loggingpkg.LogFields)
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

func cloneFields(fields loggingpkg.LogFields) loggingpkg.LogFields {
	if len(fields) == 0 {
		return nil
	}
	out := make(loggingpkg.LogFields, len(fields))
	for k, v := range fields {
		out[k] = v
	}
	return out
}

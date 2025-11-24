package runtime

import (
	"context"
	"sync"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"google.golang.org/protobuf/proto"

	configpkg "github.com/enercity/protoflow/internal/runtime/config"
	loggingpkg "github.com/enercity/protoflow/internal/runtime/logging"
)

type testPublisher struct {
	mu        sync.Mutex
	published []string
	err       error
}

func (p *testPublisher) Publish(topic string, messages ...*message.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.published = append(p.published, topic)
	return nil
}

func (p *testPublisher) Close() error { return nil }

func (p *testPublisher) Topics() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	clone := make([]string, len(p.published))
	copy(clone, p.published)
	return clone
}

type testSubscriber struct {
	err error
}

func (s *testSubscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.err != nil {
		return nil, s.err
	}
	ch := make(chan *message.Message)
	close(ch)
	return ch, nil
}

func (s *testSubscriber) Close() error { return nil }

type testValidator struct{ err error }

func (v *testValidator) Validate(_ any) error { return v.err }

type testOutbox struct {
	mu      sync.Mutex
	records []outboxRecord
	err     error
}

type outboxRecord struct {
	eventType string
	uuid      string
	payload   string
}

func (o *testOutbox) StoreOutgoingMessage(ctx context.Context, eventType, uuid, payload string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err != nil {
		return o.err
	}
	o.records = append(o.records, outboxRecord{eventType: eventType, uuid: uuid, payload: payload})
	return nil
}

func (o *testOutbox) Records() []outboxRecord {
	o.mu.Lock()
	defer o.mu.Unlock()
	clone := make([]outboxRecord, len(o.records))
	copy(clone, o.records)
	return clone
}

func newTestService(t *testing.T) *Service {
	t.Helper()
	log := newTestLogger()
	wmLogger := loggingpkg.NewWatermillAdapter(log)
	router, err := message.NewRouter(message.RouterConfig{}, wmLogger)
	if err != nil {
		t.Fatalf("router init failed: %v", err)
	}
	return &Service{
		Conf:          &configpkg.Config{},
		Logger:        log,
		router:        router,
		publisher:     &testPublisher{},
		subscriber:    &testSubscriber{},
		protoRegistry: make(map[string]func() proto.Message),
	}
}

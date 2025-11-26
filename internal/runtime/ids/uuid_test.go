package ids

import (
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestCreateUUIDFormat(t *testing.T) {
	const total = 100
	ids := make([]string, total)
	for i := 0; i < total; i++ {
		ids[i] = CreateUUID()
	}

	for i := 0; i < total; i++ {
		if len(ids[i]) != 36 {
			t.Fatalf("expected UUID length 36, got %d", len(ids[i]))
		}
		if _, err := uuid.Parse(ids[i]); err != nil {
			t.Fatalf("expected valid UUID, got %v", err)
		}
	}
}

func TestCreateUUIDConcurrentUniqueness(t *testing.T) {
	const goroutines = 10
	const perGoroutine = 20

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seen = make(map[string]struct{})
	)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				id := CreateUUID()
				if len(id) != 36 {
					t.Errorf("expected UUID length 36, got %d", len(id))
				}
				mu.Lock()
				if _, ok := seen[id]; ok {
					t.Errorf("duplicate UUID generated: %s", id)
				} else {
					seen[id] = struct{}{}
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	expected := goroutines * perGoroutine
	if len(seen) != expected {
		t.Fatalf("expected %d unique UUIDs, got %d", expected, len(seen))
	}
}

package backpressure

import (
	"sync"
	"testing"
	"time"

	"e-go04/internal/domain"
)

func TestRetryBuffer_PushAndPop(t *testing.T) {
	buf := NewRetryBuffer(10)

	if buf.Len() != 0 {
		t.Fatalf("expected empty buffer, got %d", buf.Len())
	}

	item1 := &RetryItem{
		StreamID:  "1-0",
		Event:     &domain.Event{EventID: "ev-1", OrderID: "ord-1", Country: "MX"},
		NextRetry: time.Now().Add(-1 * time.Second), // ready immediately
	}
	item2 := &RetryItem{
		StreamID:  "2-0",
		Event:     &domain.Event{EventID: "ev-2", OrderID: "ord-2", Country: "AR"},
		NextRetry: time.Now().Add(10 * time.Second), // ready in future
	}

	if !buf.Push(item1) {
		t.Fatalf("failed to push item1")
	}
	if !buf.Push(item2) {
		t.Fatalf("failed to push item2")
	}

	if buf.Len() != 2 {
		t.Fatalf("expected len 2, got %d", buf.Len())
	}
	if buf.MaxDepth() != 2 {
		t.Fatalf("expected max depth 2, got %d", buf.MaxDepth())
	}

	// PopReady with current time should return item1, not item2
	popped := buf.PopReady(time.Now())
	if popped == nil || popped.StreamID != "1-0" {
		t.Fatalf("expected item1, got %v", popped)
	}
	if buf.Len() != 1 {
		t.Fatalf("expected len 1 after pop, got %d", buf.Len())
	}

	// PopReady with current time should now return nil because item2 is in the future
	if ready := buf.PopReady(time.Now()); ready != nil {
		t.Fatalf("expected nil for future item, got %v", ready)
	}

	// Unconditional Pop should return item2
	popped2 := buf.Pop()
	if popped2 == nil || popped2.StreamID != "2-0" {
		t.Fatalf("expected item2, got %v", popped2)
	}
	if buf.Len() != 0 {
		t.Fatalf("expected empty buffer, got %d", buf.Len())
	}
}

func TestRetryBuffer_BoundedCapacity(t *testing.T) {
	buf := NewRetryBuffer(3)

	for i := 0; i < 3; i++ {
		ok := buf.Push(&RetryItem{StreamID: "msg"})
		if !ok {
			t.Fatalf("push %d failed unexpectedly", i)
		}
	}

	// 4th item must be rejected due to capacity limit
	ok := buf.Push(&RetryItem{StreamID: "overflow"})
	if ok {
		t.Fatalf("expected 4th item to be rejected")
	}

	stats := buf.Stats()
	if stats.CurrentDepth != 3 || stats.MaxDepth != 3 || stats.Capacity != 3 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestRetryBuffer_ConcurrentAccess(t *testing.T) {
	buf := NewRetryBuffer(1000)
	var wg sync.WaitGroup

	numProducers := 10
	itemsPerProducer := 50

	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < itemsPerProducer; j++ {
				buf.Push(&RetryItem{
					StreamID:  "msg",
					NextRetry: time.Now().Add(-time.Millisecond),
				})
			}
		}(i)
	}

	numConsumers := 5
	var consumedCount int
	var countMu sync.Mutex

	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				item := buf.PopReady(time.Now())
				if item != nil {
					countMu.Lock()
					consumedCount++
					countMu.Unlock()
				}
				countMu.Lock()
				done := consumedCount >= numProducers*itemsPerProducer
				countMu.Unlock()
				if done {
					break
				}
				time.Sleep(100 * time.Microsecond)
			}
		}()
	}

	wg.Wait()

	if buf.Len() != 0 {
		t.Fatalf("expected buffer to be empty after concurrent drain, got %d", buf.Len())
	}
	if buf.MaxDepth() == 0 {
		t.Fatalf("expected max depth > 0")
	}
}

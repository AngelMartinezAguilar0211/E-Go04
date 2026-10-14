package dedup

import (
	"fmt"
	"sync"
	"testing"
)

func TestSlidingLRUDeduplicator_BasicDetection(t *testing.T) {
	d := NewSlidingLRUDeduplicator(100)

	// First occurrence: unique
	if d.IsDuplicate("evt-1") {
		t.Fatalf("expected evt-1 to be unique")
	}
	if d.IsDuplicate("evt-2") {
		t.Fatalf("expected evt-2 to be unique")
	}

	// Second occurrence: duplicate
	if !d.IsDuplicate("evt-1") {
		t.Fatalf("expected evt-1 to be identified as duplicate")
	}
	if !d.IsDuplicate("evt-2") {
		t.Fatalf("expected evt-2 to be identified as duplicate")
	}

	if d.UniqueCount() != 2 {
		t.Fatalf("expected 2 unique events, got %d", d.UniqueCount())
	}
	if d.DuplicateCount() != 2 {
		t.Fatalf("expected 2 duplicate events, got %d", d.DuplicateCount())
	}
}

func TestSlidingLRUDeduplicator_Eviction(t *testing.T) {
	capacity := 3
	d := NewSlidingLRUDeduplicator(capacity)

	d.IsDuplicate("evt-1") // inserted, front: [evt-1]
	d.IsDuplicate("evt-2") // inserted, front: [evt-2, evt-1]
	d.IsDuplicate("evt-3") // inserted, front: [evt-3, evt-2, evt-1]

	if d.CurrentSize() != 3 {
		t.Fatalf("expected size 3, got %d", d.CurrentSize())
	}
	if d.EvictionCount() != 0 {
		t.Fatalf("expected 0 evictions, got %d", d.EvictionCount())
	}

	// Access evt-1 so it becomes most recently used
	if !d.IsDuplicate("evt-1") {
		t.Fatalf("expected evt-1 to be duplicate")
	}
	// State: [evt-1, evt-3, evt-2]

	// Insert evt-4 -> should evict evt-2 (oldest)
	if d.IsDuplicate("evt-4") {
		t.Fatalf("expected evt-4 to be unique")
	}

	if d.EvictionCount() != 1 {
		t.Fatalf("expected 1 eviction, got %d", d.EvictionCount())
	}

	// evt-2 was evicted, so inserting it again should be treated as unique
	if d.IsDuplicate("evt-2") {
		t.Fatalf("expected evicted evt-2 to be treated as unique again")
	}

	// evt-1 should still be present
	if !d.IsDuplicate("evt-1") {
		t.Fatalf("expected evt-1 to still be cached in the window")
	}
}

func TestSlidingLRUDeduplicator_ConcurrentAccess(t *testing.T) {
	d := NewSlidingLRUDeduplicator(1000)
	workers := 16
	iterationsPerWorker := 500

	var wg sync.WaitGroup
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < iterationsPerWorker; i++ {
				eventID := fmt.Sprintf("evt-worker-%d-%d", workerID, i%50)
				d.IsDuplicate(eventID)
			}
		}(w)
	}

	wg.Wait()

	totalCalls := int64(workers * iterationsPerWorker)
	recordedCalls := d.UniqueCount() + d.DuplicateCount()
	if recordedCalls != totalCalls {
		t.Fatalf("expected total calls %d, got %d (unique=%d, dup=%d)",
			totalCalls, recordedCalls, d.UniqueCount(), d.DuplicateCount())
	}
}

func TestSlidingLRUDeduplicator_Reset(t *testing.T) {
	d := NewSlidingLRUDeduplicator(10)
	d.IsDuplicate("e1")
	d.IsDuplicate("e1")
	d.Reset()

	if d.CurrentSize() != 0 || d.UniqueCount() != 0 || d.DuplicateCount() != 0 || d.EvictionCount() != 0 {
		t.Fatalf("reset failed to clear deduplicator state")
	}

	// e1 should now be unique again
	if d.IsDuplicate("e1") {
		t.Fatalf("expected e1 to be unique after reset")
	}
}

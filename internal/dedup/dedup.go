package dedup

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// Deduplicator defines the contract for streaming event deduplication.
type Deduplicator interface {
	// IsDuplicate checks if an eventID has already been processed within the window.
	// Returns true if duplicate (and increments duplicate counter).
	// Returns false if unique (registers the event and increments unique counter).
	IsDuplicate(eventID string) bool

	// DuplicateCount returns the total number of detected duplicate events.
	DuplicateCount() int64

	// UniqueCount returns the total number of confirmed unique events.
	UniqueCount() int64

	// EvictionCount returns the number of entries evicted due to window capacity limits.
	EvictionCount() int64

	// CurrentSize returns the current number of active entries in the deduplication window.
	CurrentSize() int

	// Capacity returns the maximum configured capacity of the deduplication window.
	Capacity() int

	// Reset clears all registered events and resets counters.
	Reset()
}

// lruEntry represents a single cached event entry in the doubly linked list.
type lruEntry struct {
	eventID string
}

// SlidingLRUDeduplicator implements a bounded sliding-window deduplicator using an LRU cache.
// It guarantees strict O(1) lookups and insertions, capping memory consumption while retaining
// recently seen event IDs across the maximum retry window.
type SlidingLRUDeduplicator struct {
	mu             sync.Mutex
	capacity       int
	items          map[string]*list.Element
	evictionList   *list.List
	uniqueCount    int64
	duplicateCount int64
	evictionCount  int64
}

// NewSlidingLRUDeduplicator creates a new bounded LRU deduplicator with the given capacity.
// If capacity <= 0, a default of 250,000 entries is applied.
func NewSlidingLRUDeduplicator(capacity int) *SlidingLRUDeduplicator {
	if capacity <= 0 {
		capacity = 250000
	}
	return &SlidingLRUDeduplicator{
		capacity:     capacity,
		items:        make(map[string]*list.Element, capacity),
		evictionList: list.New(),
	}
}

// IsDuplicate inspects the sliding window. If the event ID was previously recorded,
// it moves the entry to the front of the LRU queue and returns true.
// If not seen, it inserts the event ID, evicts the oldest entry if capacity is exceeded,
// and returns false.
func (d *SlidingLRUDeduplicator) IsDuplicate(eventID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Check if already present in the active window.
	if elem, exists := d.items[eventID]; exists {
		d.evictionList.MoveToFront(elem)
		atomic.AddInt64(&d.duplicateCount, 1)
		return true
	}

	// Register the unique event.
	atomic.AddInt64(&d.uniqueCount, 1)

	// Evict oldest item if capacity is reached.
	if d.evictionList.Len() >= d.capacity {
		oldest := d.evictionList.Back()
		if oldest != nil {
			d.evictionList.Remove(oldest)
			kv := oldest.Value.(*lruEntry)
			delete(d.items, kv.eventID)
			atomic.AddInt64(&d.evictionCount, 1)
		}
	}

	// Push new entry to the front of the list.
	entry := &lruEntry{eventID: eventID}
	elem := d.evictionList.PushFront(entry)
	d.items[eventID] = elem

	return false
}

// DuplicateCount returns the total number of detected duplicate events.
func (d *SlidingLRUDeduplicator) DuplicateCount() int64 {
	return atomic.LoadInt64(&d.duplicateCount)
}

// UniqueCount returns the total number of unique events registered.
func (d *SlidingLRUDeduplicator) UniqueCount() int64 {
	return atomic.LoadInt64(&d.uniqueCount)
}

// EvictionCount returns the number of evicted entries.
func (d *SlidingLRUDeduplicator) EvictionCount() int64 {
	return atomic.LoadInt64(&d.evictionCount)
}

// CurrentSize returns the number of entries currently stored in the window.
func (d *SlidingLRUDeduplicator) CurrentSize() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.items)
}

// Capacity returns the maximum capacity of the window.
func (d *SlidingLRUDeduplicator) Capacity() int {
	return d.capacity
}

// Reset clears all entries and counters.
func (d *SlidingLRUDeduplicator) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.items = make(map[string]*list.Element, d.capacity)
	d.evictionList.Init()
	atomic.StoreInt64(&d.uniqueCount, 0)
	atomic.StoreInt64(&d.duplicateCount, 0)
	atomic.StoreInt64(&d.evictionCount, 0)
}

package cache

import (
	"sync"
	"time"
)

type entry[V any] struct {
	value     V
	expiresAt time.Time
}

type Cache[V any] struct {
	mu      sync.Mutex
	store   map[string]entry[V]
	now     func() time.Time
	sweepAt int
}

const minSweepAt = 64

func New[V any](now func() time.Time) *Cache[V] {
	if now == nil {
		now = time.Now
	}
	return &Cache[V]{
		store:   make(map[string]entry[V]),
		now:     now,
		sweepAt: minSweepAt,
	}
}

func (c *Cache[V]) Get(key string) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.store[key]
	if !ok {
		var zero V
		return zero, false
	}
	if !c.now().Before(e.expiresAt) {
		delete(c.store, key)
		var zero V
		return zero, false
	}
	return e.value, true
}

func (c *Cache[V]) Set(key string, value V, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store[key] = entry[V]{value: value, expiresAt: c.now().Add(ttl)}
	// Amortized sweep: entries that are never Get again would otherwise
	// accumulate forever. Sweeping when the map doubles keeps Set at O(1).
	if len(c.store) >= c.sweepAt {
		now := c.now()
		for k, e := range c.store {
			if !now.Before(e.expiresAt) {
				delete(c.store, k)
			}
		}
		c.sweepAt = max(2*len(c.store), minSweepAt)
	}
}

func (c *Cache[V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store = make(map[string]entry[V])
}

package cache

import (
	"fmt"
	"testing"
	"time"
)

// Entries that expire but are never Get again must still be reclaimed
// eventually (via the amortized sweep in Set: it runs whenever the map
// doubles, so expired entries survive at most until the next doubling).
func TestSetSweepsExpiredEntries(t *testing.T) {
	now := time.Unix(0, 0)
	c := New[int](func() time.Time { return now })

	for i := 0; i < minSweepAt; i++ {
		c.Set(fmt.Sprintf("old%d", i), i, time.Second)
	}
	now = now.Add(2 * time.Second)
	for i := 0; i < minSweepAt; i++ {
		c.Set(fmt.Sprintf("new%d", i), i, time.Minute)
	}

	c.mu.Lock()
	n := len(c.store)
	_, oldAlive := c.store["old0"]
	c.mu.Unlock()
	if oldAlive {
		t.Error("expired entry old0 must have been swept")
	}
	if n != minSweepAt {
		t.Errorf("want %d live entries after sweep, got %d", minSweepAt, n)
	}
	if _, ok := c.Get("new0"); !ok {
		t.Error("fresh entry must survive the sweep")
	}
}

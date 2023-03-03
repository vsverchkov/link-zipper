package counter

import (
    "github.com/rs/zerolog/log"
    "sync"
    "sync/atomic"
)

type DCounter struct {
    Range
    mgr Manager
}

type Range struct {
    Seq atomic.Int64

    mu sync.RWMutex
    limit int64
}

func NewCounter(mgr Manager) Counter {
    return &DCounter{
        mgr: mgr,
        Range: Range{
            limit: 10,
        },
    }
}

func (c *DCounter) Next() (int64, error) {
    c.mu.RLock()
    val := c.Seq.Add(1)
    if val > c.limit {
        c.mu.RUnlock()
        return c.updateRange()
    }
    c.mu.RUnlock()
    return val, nil
}

func (c *DCounter) updateRange() (int64, error) {
    if c.mu.TryLock() {
        defer c.mu.Unlock()

        start, end, err := c.mgr.Range()
        if err != nil {
            log.Err(err)
            return 0, err
        }

        c.Seq.Store(start)
        c.limit = end

        return start, nil
    }
    c.mu.RLock()
    defer c.mu.RUnlock()
    return c.Seq.Add(1), nil
}
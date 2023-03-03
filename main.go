package main

import (
    "context"
    "fmt"
    "github.com/google/uuid"
    "github.com/rs/zerolog/log"
    "github.com/vsverchkov/link-zipper/counter"
    "sync"
    "sync/atomic"
    "time"
)

type rng struct {
	seq *atomic.Int64

	mu    *sync.RWMutex
	limit int64
}

type RngCh struct {
    SeqCh chan int64

    mu *sync.RWMutex
    start int64
    end int64
}



func main() {
    manager, err := counter.New(context.Background(), &counter.EtcdConfig{
        Hosts:          []string{"localhost:2379"},
        Timeout:        3 * time.Second,
        CounterRootKey: "link-zipper/cnt",
        BucketCnt:      10,
        NodeId:         uuid.NewString(),
    })
    if err != nil {
        log.Err(err)
    }

    counter := counter.NewCounter(manager)

    wg := sync.WaitGroup{}
    for i := 0; i < 10_00; i++ {
        wg.Add(1)
        go func() {
            next, err := counter.Next()
            wg.Done()
            if err != nil {
                log.Err(err)
                return
            }
            log.Printf("Value: %s", next)
        }()
    }
    wg.Wait()

}

func (r *RngCh) update() {
    r.mu.Lock()

    r.mu.Unlock()
}

func (r *rng) next() int64 {
    fmt.Println("Try to read")
    r.mu.RLock()
    val := r.seq.Add(1)
    if val > r.limit {
        r.mu.RUnlock()
        return r.updRng()
    }
    fmt.Println("Read success")
    r.mu.RUnlock()
    return val
}

func (r *rng) updRng() int64 {
    fmt.Println("Try UPD")
    if r.mu.TryLock() {
        fmt.Println("Took Lock")
        defer r.mu.Unlock()
        time.Sleep(2 * time.Second)
        r.seq = &atomic.Int64{}
        r.limit = 10
        fmt.Println("UPD via look success")
        return r.seq.Load()
    }
    r.mu.RLock()
    defer r.mu.RUnlock()
    fmt.Println("UPD via wait success")
    return r.seq.Add(1)
}
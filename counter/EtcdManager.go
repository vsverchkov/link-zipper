package counter

import (
    "bytes"
    "context"
    "encoding/binary"
    "errors"
    "github.com/rs/zerolog/log"
    client "go.etcd.io/etcd/client/v3"
    cliCncr "go.etcd.io/etcd/client/v3/concurrency"
    "math/rand"

    "strconv"
    "sync"
    "time"
)

type EtcdConfig struct {
    Hosts []string
    Timeout time.Duration
    CounterRootKey string
    BucketPfix string
    BucketCnt int
    BucketCtxKey string

    NodeId string
}

type EtcdManager struct {
    cli *client.Client
    buckets *sync.Map
    config *EtcdConfig
    ctx context.Context
}

type bucket struct {
    mu *cliCncr.Mutex
    key string
}

var ErrBucketNotFound = errors.New("etcd.bucket: not found")
var ErrMoreThatOneKey = errors.New("etcd.bucket: more that one key found")

func New(ctx context.Context, config *EtcdConfig) (*EtcdManager, error) {
    if err := validateConfig(config); err != nil {
        log.Err(err)
        return nil, err
    }

    if config.CounterRootKey == "" {
        config.CounterRootKey = "/link-zipper/cnt"
    }

    if config.BucketPfix == "" {
        config.BucketPfix = config.CounterRootKey + "/bucket"
    }

    if config.Timeout == 0 {
        config.Timeout = time.Second * 3
    }

    cli, err := client.New(client.Config{
        Endpoints: config.Hosts,
        Context:   ctx,
    })

    if err != nil {
        log.Err(err)
        return nil, err
    }

    buckets, err := initBuckets(ctx, cli, config)
    if err != nil {
        log.Err(err)
        return nil, err
    }

    return &EtcdManager{
        cli: cli,
        buckets: buckets,
        config: config,
        ctx: ctx,
    }, nil
}

func initBuckets(ctx context.Context, cli *client.Client, config *EtcdConfig) (*sync.Map, error) {
    zeroStr := strconv.Itoa(0)
    errCh := make(chan error)
    wg := sync.WaitGroup{}
    buckets := sync.Map{}
    for i := 0; i < config.BucketCnt; i++ {
        wg.Add(1)
        go func(curr int) {
            bucketKey := config.BucketPfix + "/" + strconv.Itoa(curr)
            _, err := cli.Put(ctx, bucketKey, zeroStr)
            if err != nil {
                log.Err(err)
                errCh <- err
            }

            session, err := cliCncr.NewSession(cli)
            if err != nil {
                log.Err(err)
                errCh <- err
            }

            bucketLockKey := bucketKey + "/lock"
            lock := cliCncr.NewMutex(session, bucketLockKey)
            buckets.Store(curr, &bucket{
                mu: lock,
                key: bucketKey,
            })
            wg.Done()
        }(i)
    }
    wg.Wait()
    if len(errCh) > 0 {
        return nil, <- errCh
    }
    return &buckets, nil
}

func (m *EtcdManager) Range() (start, end int64, err error) {
    for {
        bucketID := rand.Intn(m.config.BucketCnt + 1)
        bucketNode, err := m.lockedBucket(bucketID)
        log.Printf("locked bucket %s", bucketNode)
        if errors.Is(err, ErrBucketNotFound) || errors.Is(err, cliCncr.ErrLocked) {
            log.Print("try with another bucket")
            continue
        }

        if err != nil {
            log.Err(err)
            return 0, 0, err
        }

        count, err := m.cli.KV.Get(m.ctx, bucketNode.key)
        if err != nil {
            log.Err(err)
            return 0, 0, err
        }
        if count.Count > 1 {
            log.Err(ErrMoreThatOneKey)
            return 0, 0, ErrMoreThatOneKey
        }
        value, err := binary.ReadVarint(bytes.NewReader(count.Kvs[0].Value))
        if err != nil {
            err := bucketNode.mu.Unlock(m.ctx)
            if err != nil {
                log.Err(err)
                return 0, 0, err
            }
            log.Err(ErrMoreThatOneKey)
            return 0, 0, ErrMoreThatOneKey
        }
        start = value
        end = value + 10 //TODO: fix increment
        _, err = m.cli.KV.Put(m.ctx, bucketNode.key, strconv.FormatInt(end, 10))
        if err != nil {
            log.Err(err)
            return 0, 0, err
        }

        log.Printf("ranged start %s, end %s", start, end)
        return start, end, nil
    }
}

func (m *EtcdManager) lockedBucket(ID int) (*bucket, error) {
    bucketNode, ok := m.buckets.Load(ID)
    if !ok {
        log.Err(ErrBucketNotFound)
        return nil, ErrBucketNotFound
    }

    bucket := bucketNode.(*bucket)
    err := bucket.mu.TryLock(m.ctx)
    if err == cliCncr.ErrLocked {
        return nil, cliCncr.ErrLocked
    }
    return bucket, nil
}

func validateConfig(config *EtcdConfig) error {
    if len(config.Hosts) < 1 {
        return errors.New("hosts does not configure")
    }
    return nil
}
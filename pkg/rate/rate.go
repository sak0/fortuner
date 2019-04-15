package rate

import (
	"sync"
	"context"
	"time"
	"github.com/golang/glog"
)

type Limiter struct {
	limit 		float64
	burst 		int
	mu 			sync.Mutex
	redisCli 	*RedisKeeper
}

func (l *Limiter)Limit() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.limit
}

func (l *Limiter)tryReserve (done <-chan interface{}) chan bool {
	outStream := make(chan bool)
	go func() {
		defer close(outStream)
		for {
			select {
			case <-done:
				return
			case <-time.After(500 * time.Millisecond):
				res, err := l.redisCli.Eval(int(l.limit), l.burst, 1)
				select {
				case <-done:
					return
				default:
					if err != nil {
						outStream<- false
					}
					outStream<- res
				}

			}
		}
	}()

	return outStream
}

func (l *Limiter)Wait(ctx context.Context) error {
	glog.V(5).Infof("MyRate wait debugInfo...")
	stopCh := make(chan interface{})
	defer close(stopCh)
	resultCh := l.tryReserve(stopCh)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res, ok := <-resultCh:
			if !ok {
				return nil
			}
			if res {
				return nil
			}
		}
	}

	return nil
}

func (l *Limiter)Allow() bool {
	return true
}

func (l *Limiter)AllowN(now time.Time, n int) bool {
	return true
}

func NewLimiter(limit float64, b int) (*Limiter, error) {
	redisClient, err := NewRedisClient()
	if err != nil {
		glog.Errorf("Get redis client failed: %v\n", err)
		return nil, err
	}

	return &Limiter{
		limit:limit,
		burst:b,
		mu:sync.Mutex{},
		redisCli:redisClient,
	}, nil
}
package cron

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type Orchestrator struct {
	cfg     *config
	crons   map[string]*Cron
	pool    *WorkerPool
	mu      sync.RWMutex
	started bool
}

func NewOrchestrator(workers int, opts ...Option) *Orchestrator {
	cfg := defaultConfig()
	for _, o := range opts {
		o(cfg)
	}
	return &Orchestrator{
		cfg:   cfg,
		crons: make(map[string]*Cron),
		pool:  NewWorkerPool(workers),
	}
}

func (o *Orchestrator) Register(name string, c *Cron) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if _, exists := o.crons[name]; exists {
		return fmt.Errorf("orchestrator: cron %q already registered", name)
	}
	o.crons[name] = c

	if o.started {
		c.Start()
	}
	return nil
}

func (o *Orchestrator) Unregister(name string) {
	o.mu.Lock()
	c, ok := o.crons[name]
	if ok {
		delete(o.crons, name)
	}
	o.mu.Unlock()

	if ok {
		<-c.Stop().Done()
	}
}

func (o *Orchestrator) Cron(name string) (*Cron, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	c, ok := o.crons[name]
	return c, ok
}

func (o *Orchestrator) AddDistributedJob(cronName, spec string, job Job, opts ...AddOption) (EntryID, error) {
	o.mu.RLock()
	c, ok := o.crons[cronName]
	o.mu.RUnlock()
	if !ok {
		return 0, fmt.Errorf("orchestrator: cron %q not found", cronName)
	}

	ac := &addConfig{name: fmt.Sprintf("%s/job", cronName)}
	for _, opt := range opts {
		opt(ac)
	}

	lockKey := fmt.Sprintf("%s/%s", cronName, ac.name)
	locker := o.cfg.locker
	logger := o.cfg.logger

	wrapped := JobFunc(func(ctx context.Context) error {
		ok, err := locker.AcquireLock(ctx, lockKey, lockTTL)
		if err != nil {
			return fmt.Errorf("lock acquire: %w", err)
		}
		if !ok {
			logger.Debug("orchestrator: lock not acquired, skipping",
				slog.String("key", lockKey))
			return nil
		}
		defer func() {
			if rerr := locker.ReleaseLock(ctx, lockKey); rerr != nil {
				logger.Warn("orchestrator: failed to release lock",
					slog.String("key", lockKey), slog.Any("err", rerr))
			}
		}()

		result := o.pool.Submit(ctx, job)
		select {
		case err := <-result:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	return c.AddJob(spec, wrapped, WithName(ac.name))
}

func (o *Orchestrator) Start() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.started {
		return
	}
	o.started = true
	for _, c := range o.crons {
		c.Start()
	}
}

func (o *Orchestrator) Stop(ctx context.Context) error {
	o.mu.Lock()
	crons := make([]*Cron, 0, len(o.crons))
	for _, c := range o.crons {
		crons = append(crons, c)
	}
	o.started = false
	o.mu.Unlock()

	var wg sync.WaitGroup
	for _, c := range crons {
		wg.Add(1)
		go func(cr *Cron) {
			defer wg.Done()
			select {
			case <-cr.Stop().Done():
			case <-ctx.Done():
			}
		}(c)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		o.pool.Close()
		return nil
	case <-ctx.Done():
		go func() {
			<-done
			o.pool.Close()
		}()
		return ctx.Err()
	}
}

const lockTTL = 30 * time.Second

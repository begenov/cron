package cron

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

type Cron struct {
	cfg     *config
	entries []*Entry
	names   map[EntryID]string
	running map[EntryID]bool

	add    chan *Entry
	remove chan EntryID
	snap   chan chan []*Entry
	stop   chan struct{}
	done   chan struct{}

	mu       sync.Mutex
	stopOnce sync.Once
	started  bool
}

func New(opts ...Option) *Cron {
	cfg := defaultConfig()
	for _, o := range opts {
		o(cfg)
	}
	return &Cron{
		cfg:     cfg,
		names:   make(map[EntryID]string),
		running: make(map[EntryID]bool),
		add:     make(chan *Entry),
		remove:  make(chan EntryID),
		snap:    make(chan chan []*Entry),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

func (c *Cron) AddJob(spec string, job Job, opts ...AddOption) (EntryID, error) {
	sched, err := Parse(spec)
	if err != nil {
		return 0, fmt.Errorf("cron: %w", err)
	}
	return c.Schedule(sched, job, opts...), nil
}

func (c *Cron) AddFunc(spec string, fn func(context.Context) error, opts ...AddOption) (EntryID, error) {
	return c.AddJob(spec, JobFunc(fn), opts...)
}

func (c *Cron) Schedule(sched Schedule, job Job, opts ...AddOption) EntryID {
	ac := &addConfig{}
	for _, o := range opts {
		o(ac)
	}
	e := &Entry{
		ID:          nextEntryID(),
		Schedule:    sched,
		Job:         job,
		retryPolicy: ac.retryPolicy,
	}

	name := ac.name
	if name == "" {
		name = fmt.Sprintf("job-%d", e.ID)
	}

	c.mu.Lock()
	c.names[e.ID] = name
	started := c.started
	c.mu.Unlock()

	if started {
		c.add <- e
	} else {
		c.mu.Lock()
		c.entries = append(c.entries, e)
		c.mu.Unlock()
	}

	return e.ID
}

func (c *Cron) Remove(id EntryID) {
	c.mu.Lock()
	started := c.started
	c.mu.Unlock()

	if started {
		c.remove <- id
	} else {
		c.mu.Lock()
		c.removeEntry(id)
		c.mu.Unlock()
	}
}

func (c *Cron) Entries() []*Entry {
	c.mu.Lock()
	started := c.started
	c.mu.Unlock()

	if started {
		reply := make(chan []*Entry)
		c.snap <- reply
		return <-reply
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return copyEntries(c.entries)
}

func (c *Cron) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return
	}
	c.started = true
	c.stop = make(chan struct{})
	c.done = make(chan struct{})
	c.stopOnce = sync.Once{}
	go c.run()
}

func (c *Cron) Stop() context.Context {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	c.mu.Unlock()

	c.stopOnce.Do(func() { close(c.stop) })
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-c.done
		cancel()
	}()
	return ctx
}

func (c *Cron) run() {
	defer close(c.done)

	now := c.now()
	for _, e := range c.entries {
		e.Next = e.Schedule.Next(now)
	}

	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	var wg sync.WaitGroup

	for {
		sortEntries(c.entries)

		var timer *time.Timer
		if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			timer = time.NewTimer(100000 * time.Hour)
		} else {
			timer = time.NewTimer(c.entries[0].Next.Sub(c.now()))
		}

		select {
		case <-c.stop:
			timer.Stop()
			cancelCtx()

			waitDone := make(chan struct{})
			go func() {
				wg.Wait()
				close(waitDone)
			}()
			for {
				select {
				case <-waitDone:
					c.mu.Lock()
					c.started = false
					c.mu.Unlock()
					return
				case e := <-c.add:
					e.Next = e.Schedule.Next(c.now())
					c.entries = append(c.entries, e)
				case id := <-c.remove:
					c.removeEntry(id)
				case reply := <-c.snap:
					reply <- copyEntries(c.entries)
				}
			}

		case now = <-timer.C:
			now = now.In(c.cfg.location)
			for _, e := range c.entries {
				if e.Next.After(now) || e.Next.IsZero() {
					break
				}
				e.Prev = e.Next
				e.Next = e.Schedule.Next(now)

				c.mu.Lock()
				name := c.names[e.ID]
				if !c.cfg.concurrent {
					if c.running[e.ID] {
						c.mu.Unlock()
						c.cfg.logger.Debug("cron: skipping job (already running)", slog.Any("id", e.ID), slog.String("name", name))
						continue
					}
					c.running[e.ID] = true
				}
				c.mu.Unlock()

				wg.Add(1)
				go func(entry *Entry, jobName string, fired time.Time) {
					defer wg.Done()
					if !c.cfg.concurrent {
						defer func() {
							c.mu.Lock()
							delete(c.running, entry.ID)
							c.mu.Unlock()
						}()
					}
					c.runEntry(ctx, entry, jobName, fired)
				}(e, name, now)
			}

		case e := <-c.add:
			timer.Stop()
			e.Next = e.Schedule.Next(c.now())
			c.entries = append(c.entries, e)

		case id := <-c.remove:
			timer.Stop()
			c.removeEntry(id)

		case reply := <-c.snap:
			reply <- copyEntries(c.entries)
		}
	}
}

func (c *Cron) runEntry(ctx context.Context, e *Entry, name string, scheduledAt time.Time) {
	start := time.Now()

	c.cfg.logger.Info("cron: running job", slog.String("name", name), slog.Any("id", e.ID))

	err := e.run(ctx)

	rec := RunRecord{
		EntryID:     e.ID,
		JobName:     name,
		ScheduledAt: scheduledAt,
		StartedAt:   start,
		FinishedAt:  time.Now(),
		Status:      StatusSuccess,
	}

	if err != nil {
		rec.Status = StatusFailure
		rec.Err = err.Error()
		c.cfg.logger.Error("cron: job failed", slog.String("name", name), slog.Any("id", e.ID), slog.Any("err", err))
	}

	if c.cfg.history != nil {
		if saveErr := c.cfg.history.SaveRun(ctx, rec); saveErr != nil {
			c.cfg.logger.Warn("cron: failed to save run record", slog.Any("err", saveErr))
		}
	}
}

func (c *Cron) removeEntry(id EntryID) {
	entries := make([]*Entry, 0, len(c.entries))
	for _, e := range c.entries {
		if e.ID != id {
			entries = append(entries, e)
		}
	}
	c.entries = entries
	delete(c.names, id)
}

func (c *Cron) now() time.Time {
	return time.Now().In(c.cfg.location)
}

func sortEntries(entries []*Entry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Next.IsZero() {
			return false
		}
		if entries[j].Next.IsZero() {
			return true
		}
		return entries[i].Next.Before(entries[j].Next)
	})
}

func copyEntries(src []*Entry) []*Entry {
	out := make([]*Entry, len(src))
	for i, e := range src {
		cp := *e
		out[i] = &cp
	}
	return out
}

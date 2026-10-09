package cron

import (
	"context"
	"sync"
)

type task struct {
	ctx  context.Context
	job  Job
	done chan<- error
}

type WorkerPool struct {
	queue chan task
	wg    sync.WaitGroup
}

func NewWorkerPool(n int) *WorkerPool {
	if n <= 0 {
		panic("cron: WorkerPool requires n > 0")
	}
	p := &WorkerPool{queue: make(chan task, n*4)}
	for range n {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

func (p *WorkerPool) worker() {
	defer p.wg.Done()
	for t := range p.queue {
		t.done <- t.job.Run(t.ctx)
	}
}

func (p *WorkerPool) Submit(ctx context.Context, job Job) <-chan error {
	done := make(chan error, 1)
	select {
	case p.queue <- task{ctx: ctx, job: job, done: done}:
	case <-ctx.Done():
		done <- ctx.Err()
	}
	return done
}

func (p *WorkerPool) Close() {
	close(p.queue)
	p.wg.Wait()
}

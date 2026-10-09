package cron

import "context"

type Job interface {
	Run(ctx context.Context) error
}

type JobFunc func(ctx context.Context) error

func (f JobFunc) Run(ctx context.Context) error { return f(ctx) }

type FuncJob func()

func (f FuncJob) Run(_ context.Context) error {
	f()
	return nil
}

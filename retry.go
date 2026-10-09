package cron

import (
	"context"
	"time"
)

type RetryPolicy struct {
	MaxAttempts int
	Delay       time.Duration
	Multiplier  float64
}

func (r *RetryPolicy) execute(ctx context.Context, job Job) error {
	mult := r.Multiplier
	if mult <= 0 {
		mult = 1
	}
	delay := r.Delay
	var err error
	for attempt := 0; attempt < r.MaxAttempts; attempt++ {
		if err = job.Run(ctx); err == nil {
			return nil
		}
		if attempt < r.MaxAttempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			delay = time.Duration(float64(delay) * mult)
		}
	}
	return err
}

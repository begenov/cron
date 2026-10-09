package cron

import (
	"context"
	"sync/atomic"
	"time"
)

type EntryID uint64

var entryIDCounter atomic.Uint64

func nextEntryID() EntryID {
	return EntryID(entryIDCounter.Add(1))
}

type Entry struct {
	ID       EntryID
	Schedule Schedule
	Job      Job
	Next     time.Time
	Prev     time.Time

	retryPolicy *RetryPolicy
}

func (e *Entry) run(ctx context.Context) error {
	if e.retryPolicy == nil || e.retryPolicy.MaxAttempts <= 1 {
		return e.Job.Run(ctx)
	}
	return e.retryPolicy.execute(ctx, e.Job)
}

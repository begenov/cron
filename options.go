package cron

import (
	"log/slog"
	"time"
)

type Option func(*config)

type config struct {
	location   *time.Location
	history    HistoryStorage
	locker     Locker
	logger     *slog.Logger
	concurrent bool
}

func defaultConfig() *config {
	mem := NewMemoryStorage()
	return &config{
		location:   time.Local,
		history:    mem,
		locker:     mem,
		logger:     slog.Default(),
		concurrent: false,
	}
}

func WithLocation(loc *time.Location) Option {
	return func(c *config) { c.location = loc }
}

func WithStorage(s Storage) Option {
	return func(c *config) {
		c.history = s
		c.locker = s
	}
}

func WithHistory(h HistoryStorage) Option {
	return func(c *config) { c.history = h }
}

func WithLocker(l Locker) Option {
	return func(c *config) { c.locker = l }
}

func WithLogger(l *slog.Logger) Option {
	return func(c *config) { c.logger = l }
}

func WithConcurrentRuns() Option {
	return func(c *config) { c.concurrent = true }
}

type AddOption func(*addConfig)

type addConfig struct {
	name        string
	retryPolicy *RetryPolicy
}

func WithName(name string) AddOption {
	return func(c *addConfig) { c.name = name }
}

func WithRetry(policy RetryPolicy) AddOption {
	return func(c *addConfig) { c.retryPolicy = &policy }
}

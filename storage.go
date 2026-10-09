package cron

import (
	"context"
	"sync"
	"time"
)

type RunStatus string

const (
	StatusSuccess RunStatus = "success"
	StatusFailure RunStatus = "failure"
	StatusSkipped RunStatus = "skipped"
)

type RunRecord struct {
	EntryID     EntryID   `json:"entry_id"`
	JobName     string    `json:"job_name"`
	ScheduledAt time.Time `json:"scheduled_at"`
	StartedAt   time.Time `json:"started_at"`
	FinishedAt  time.Time `json:"finished_at"`
	Status      RunStatus `json:"status"`
	Err         string    `json:"err,omitempty"`
}

type HistoryStorage interface {
	SaveRun(ctx context.Context, r RunRecord) error
	ListRuns(ctx context.Context, entryID EntryID, limit int) ([]RunRecord, error)
}

type Locker interface {
	AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, error)
	ReleaseLock(ctx context.Context, key string) error
}

type Storage interface {
	HistoryStorage
	Locker
}

type MemoryStorage struct {
	mu    sync.Mutex
	runs  map[EntryID][]RunRecord
	locks map[string]time.Time
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		runs:  make(map[EntryID][]RunRecord),
		locks: make(map[string]time.Time),
	}
}

func (m *MemoryStorage) SaveRun(_ context.Context, r RunRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runs[r.EntryID] = append([]RunRecord{r}, m.runs[r.EntryID]...)
	return nil
}

func (m *MemoryStorage) ListRuns(_ context.Context, id EntryID, limit int) ([]RunRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	records := m.runs[id]
	if limit > 0 && len(records) > limit {
		records = records[:limit]
	}
	out := make([]RunRecord, len(records))
	copy(out, records)
	return out, nil
}

func (m *MemoryStorage) AcquireLock(_ context.Context, key string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if exp, ok := m.locks[key]; ok && time.Now().Before(exp) {
		return false, nil
	}
	m.locks[key] = time.Now().Add(ttl)
	return true, nil
}

func (m *MemoryStorage) ReleaseLock(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.locks, key)
	return nil
}

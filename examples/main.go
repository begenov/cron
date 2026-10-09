package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/begenov/cron"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	fmt.Println("=== 1. Базовый планировщик ===")
	basicExample(logger)

	fmt.Println("\n=== 2. Retry + история выполнений ===")
	retryExample(logger)

	fmt.Println("\n=== 3. Оркестратор с несколькими планировщиками ===")
	orchestratorExample(logger)

	fmt.Println("\n=== 4. Интерфейсы Storage — mix & match ===")
	storageInterfacesExample(logger)

	fmt.Println("\n=== 5. Graceful shutdown по SIGTERM ===")
	gracefulExample(logger)
}

func basicExample(logger *slog.Logger) {
	c := cron.New(
		cron.WithLogger(logger),
		cron.WithLocation(time.Local),
	)

	c.AddFunc("* * * * *", func(_ context.Context) error {
		fmt.Println("[basic] каждую минуту")
		return nil
	}, cron.WithName("every-minute"))

	c.AddFunc("@every 2s", func(_ context.Context) error {
		fmt.Println("[basic] каждые 2 секунды")
		return nil
	}, cron.WithName("every-2s"))

	c.AddFunc("*/3 * * * * *", func(_ context.Context) error {
		fmt.Println("[basic] каждые 3 секунды (6-польный cron)")
		return nil
	}, cron.WithName("every-3s"))

	c.Start()
	time.Sleep(7 * time.Second)
	<-c.Stop().Done()

	fmt.Printf("[basic] всего задач в snapshot: %d\n", len(c.Entries()))
}

func retryExample(logger *slog.Logger) {
	storage := cron.NewMemoryStorage()
	c := cron.New(
		cron.WithLogger(logger),
		cron.WithStorage(storage),
	)

	attempts := 0
	id, _ := c.AddFunc("@every 3s",
		func(_ context.Context) error {
			attempts++
			if attempts < 3 {
				fmt.Printf("[retry] попытка %d — ошибка\n", attempts)
				return errors.New("временная ошибка")
			}
			fmt.Printf("[retry] попытка %d — успех\n", attempts)
			return nil
		},
		cron.WithName("flaky-job"),
		cron.WithRetry(cron.RetryPolicy{
			MaxAttempts: 3,
			Delay:       200 * time.Millisecond,
			Multiplier:  2.0,
		}),
	)

	c.Start()
	time.Sleep(5 * time.Second)
	<-c.Stop().Done()

	records, _ := storage.ListRuns(context.Background(), id, 5)
	fmt.Printf("[retry] история (%d записей):\n", len(records))
	for _, r := range records {
		dur := r.FinishedAt.Sub(r.StartedAt).Round(time.Millisecond)
		fmt.Printf("  [%s] %s  длительность=%s  ошибка=%q\n",
			r.Status, r.StartedAt.Format("15:04:05"), dur, r.Err)
	}
}

func orchestratorExample(logger *slog.Logger) {
	orch := cron.NewOrchestrator(
		4,
		cron.WithLogger(logger),
		cron.WithStorage(cron.NewMemoryStorage()),
	)

	svcA := cron.New(cron.WithLogger(logger))
	svcA.AddFunc("@every 1s", func(_ context.Context) error {
		fmt.Println("[svcA] heartbeat")
		return nil
	}, cron.WithName("heartbeat"))

	svcB := cron.New(cron.WithLogger(logger))
	svcB.AddFunc("@every 2s", func(_ context.Context) error {
		fmt.Println("[svcB] cleanup")
		return nil
	}, cron.WithName("cleanup"))

	orch.Register("service-a", svcA)
	orch.Register("service-b", svcB)

	orch.AddDistributedJob(
		"service-a",
		"@every 2s",
		cron.JobFunc(func(_ context.Context) error {
			fmt.Println("[distributed] выполняется только на одной ноде")
			return nil
		}),
		cron.WithName("distributed-report"),
	)

	orch.Start()
	time.Sleep(5 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := orch.Stop(ctx); err != nil {
		fmt.Printf("[orchestrator] ошибка при остановке: %v\n", err)
	}
	fmt.Println("[orchestrator] остановлен")
}

func storageInterfacesExample(logger *slog.Logger) {
	mem := cron.NewMemoryStorage()

	c := cron.New(
		cron.WithStorage(mem),
		cron.WithLogger(logger),
	)

	var id cron.EntryID
	id, _ = c.AddFunc("@every 1s", func(_ context.Context) error {
		fmt.Println("[storage] job ran")
		return nil
	}, cron.WithName("demo"))

	c.Start()
	time.Sleep(3 * time.Second)
	<-c.Stop().Done()

	records, _ := mem.ListRuns(context.Background(), id, 0)
	fmt.Printf("[storage] записей в истории: %d\n", len(records))
	for _, r := range records {
		fmt.Printf("  [%s] %s\n", r.Status, r.StartedAt.Format("15:04:05"))
	}

	fmt.Println("[storage] пример своего Locker:")
	var _ cron.Locker = (*noopLocker)(nil)
	fmt.Println("  noopLocker реализует cron.Locker — ОК")

	var _ cron.HistoryStorage = (*noopHistory)(nil)
	fmt.Println("  noopHistory реализует cron.HistoryStorage — ОК")
}

type noopLocker struct{}

func (noopLocker) AcquireLock(_ context.Context, _ string, _ time.Duration) (bool, error) {
	return true, nil
}
func (noopLocker) ReleaseLock(_ context.Context, _ string) error { return nil }

type noopHistory struct{}

func (noopHistory) SaveRun(_ context.Context, _ cron.RunRecord) error { return nil }
func (noopHistory) ListRuns(_ context.Context, _ cron.EntryID, _ int) ([]cron.RunRecord, error) {
	return nil, nil
}

func gracefulExample(logger *slog.Logger) {
	c := cron.New(cron.WithLogger(logger))

	c.AddFunc("@every 1s", func(_ context.Context) error {
		fmt.Println("[graceful] tick")
		return nil
	}, cron.WithName("ticker"))

	c.Start()
	fmt.Println("[graceful] работает. Нажмите Ctrl+C для остановки (или подождите 4s)")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		fmt.Printf("[graceful] получен сигнал %s\n", sig)
	case <-time.After(4 * time.Second):
		fmt.Println("[graceful] таймаут, останавливаемся")
	}

	stopCtx := c.Stop()
	<-stopCtx.Done()
	fmt.Println("[graceful] все задачи завершены, выходим")
}

package main

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Antonyiuri/jungle-gaming-backend/internal/config"
	"go.uber.org/fx"
)

func testConfig() config.Config {
	return config.Config{ShutdownTimeout: time.Second, ProcessingTimeout: time.Second, ReferenceTTL: time.Hour}
}

// ValidateApp type-checks the dependency graph without running any constructor, so no database,
// identity provider or broker is needed.
func TestDependencyGraphIsComplete(t *testing.T) {
	if err := fx.ValidateApp(options(testConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)))); err != nil {
		t.Fatalf("the application graph is invalid: %v", err)
	}
}

// Guards the guard: validation must actually fail when a dependency is missing.
func TestValidationDetectsAMissingDependency(t *testing.T) {
	incomplete := fx.Options(fx.Supply(testConfig(), slog.New(slog.NewTextHandler(io.Discard, nil))), fx.Invoke(serveHTTP))
	if err := fx.ValidateApp(incomplete); err == nil {
		t.Fatal("a graph without its providers must not validate")
	}
}

func TestSupervisedWorkerIsRestartedAfterAPanicAndStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var runs atomic.Int32
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		supervise(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), "test", func(ctx context.Context) {
			if runs.Add(1) == 1 {
				panic("boom")
			}
			<-ctx.Done()
		})
	}()
	deadline := time.Now().Add(10 * time.Second)
	for runs.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if runs.Load() < 2 {
		t.Fatal("the loop must be restarted after a panic")
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("the supervisor must stop when its context ends")
	}
}

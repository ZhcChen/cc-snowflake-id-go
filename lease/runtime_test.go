package lease

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestRuntimeStopCancelsJoinsThenCloses(t *testing.T) {
	generator := newRuntimeTestGenerator()
	runtime, err := StartRuntime(context.Background(), generator)
	if err != nil {
		t.Fatalf("StartRuntime() error = %v", err)
	}
	<-generator.refreshStarted

	if err := runtime.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := runtime.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop() error = %v", err)
	}
	if cause := context.Cause(runtime.Context()); !errors.Is(cause, context.Canceled) {
		t.Fatalf("runtime cause = %v, want context.Canceled", cause)
	}
	if operations := generator.operationSnapshot(); !equalStrings(operations, []string{"refresh-start", "refresh-end", "close"}) {
		t.Fatalf("operations = %#v, want cancel/join/close order", operations)
	}
	if generator.closeCallCount() != 1 {
		t.Fatalf("close calls = %d, want 1", generator.closeCallCount())
	}
}

func TestRuntimeRefreshFailureCancelsWorkAndPreservesRootCause(t *testing.T) {
	refreshErr := errors.New("refresh store failed")
	closeErr := errors.New("close store failed")
	generator := newRuntimeTestGenerator()
	generator.refreshErr = refreshErr
	generator.closeErr = closeErr
	generator.failRefresh = make(chan struct{})

	runtime, err := StartRuntime(context.Background(), generator)
	if err != nil {
		t.Fatalf("StartRuntime() error = %v", err)
	}
	<-generator.refreshStarted
	close(generator.failRefresh)
	<-runtime.Done()

	if err := runtime.Err(); !errors.Is(err, refreshErr) {
		t.Fatalf("Err() = %v, want refresh root cause", err)
	}
	if cause := context.Cause(runtime.Context()); !errors.Is(cause, refreshErr) {
		t.Fatalf("runtime cause = %v, want refresh root cause", cause)
	}
	if err := runtime.Stop(context.Background()); !errors.Is(err, refreshErr) || !errors.Is(err, closeErr) {
		t.Fatalf("Stop() error = %v, want joined refresh and close errors", err)
	}
	if operations := generator.operationSnapshot(); !equalStrings(operations, []string{"refresh-start", "refresh-end", "close"}) {
		t.Fatalf("operations = %#v, want refresh completion before close", operations)
	}
}

type runtimeTestGenerator struct {
	mu sync.Mutex

	refreshStarted chan struct{}
	failRefresh    chan struct{}
	refreshErr     error
	closeErr       error
	closeCalls     int
	operations     []string
}

func newRuntimeTestGenerator() *runtimeTestGenerator {
	return &runtimeTestGenerator{refreshStarted: make(chan struct{})}
}

func (g *runtimeTestGenerator) RunRefreshLoop(ctx context.Context) error {
	g.record("refresh-start")
	close(g.refreshStarted)
	var err error
	if g.failRefresh != nil {
		select {
		case <-g.failRefresh:
			err = g.refreshErr
		case <-ctx.Done():
			err = ctx.Err()
		}
	} else {
		<-ctx.Done()
		err = ctx.Err()
	}
	g.record("refresh-end")
	return err
}

func (g *runtimeTestGenerator) Close(context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closeCalls++
	g.operations = append(g.operations, "close")
	return g.closeErr
}

func (g *runtimeTestGenerator) record(operation string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.operations = append(g.operations, operation)
}

func (g *runtimeTestGenerator) operationSnapshot() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.operations...)
}

func (g *runtimeTestGenerator) closeCallCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closeCalls
}

func equalStrings(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

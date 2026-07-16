package lease

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestStartRuntimeRejectsNilGenerator(t *testing.T) {
	if _, err := StartRuntime(context.Background(), nil); !errors.Is(err, ErrInvalidLeaseConfig) {
		t.Fatalf("StartRuntime(nil) error = %v, want ErrInvalidLeaseConfig", err)
	}
}

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

func TestRuntimeNilAndZeroValueAccessorsAreSafe(t *testing.T) {
	var nilRuntime *Runtime
	if nilRuntime.Context() == nil {
		t.Fatal("nil runtime Context() returned nil")
	}
	select {
	case <-nilRuntime.Done():
	default:
		t.Fatal("nil runtime Done() should return a closed channel")
	}
	if err := nilRuntime.Err(); err != nil {
		t.Fatalf("nil runtime Err() = %v, want nil", err)
	}
	if err := nilRuntime.Stop(nil); err != nil {
		t.Fatalf("nil runtime Stop() = %v, want nil", err)
	}

	zeroRuntime := &Runtime{}
	if zeroRuntime.Context() == nil {
		t.Fatal("zero runtime Context() returned nil")
	}
	select {
	case <-zeroRuntime.Done():
	default:
		t.Fatal("zero runtime Done() should return a closed channel")
	}
	if err := zeroRuntime.Err(); err != nil {
		t.Fatalf("zero runtime Err() = %v, want nil", err)
	}
}

func TestRuntimeUnexpectedRefreshLoopStopBecomesTerminalError(t *testing.T) {
	generator := newRuntimeTestGenerator()
	generator.returnImmediately = true

	runtime, err := StartRuntime(nil, generator)
	if err != nil {
		t.Fatalf("StartRuntime(nil parent) error = %v", err)
	}
	<-runtime.Done()

	if err := runtime.Err(); !errors.Is(err, ErrRefreshLoopStopped) {
		t.Fatalf("Err() = %v, want ErrRefreshLoopStopped", err)
	}
	if cause := context.Cause(runtime.Context()); !errors.Is(cause, ErrRefreshLoopStopped) {
		t.Fatalf("runtime cause = %v, want ErrRefreshLoopStopped", cause)
	}
	if err := runtime.Stop(context.Background()); !errors.Is(err, ErrRefreshLoopStopped) {
		t.Fatalf("Stop() error = %v, want ErrRefreshLoopStopped", err)
	}
	if operations := generator.operationSnapshot(); !equalStrings(operations, []string{"refresh-start", "refresh-end", "close"}) {
		t.Fatalf("operations = %#v, want refresh completion before close", operations)
	}
}

type runtimeTestGenerator struct {
	mu sync.Mutex

	refreshStarted    chan struct{}
	failRefresh       chan struct{}
	refreshErr        error
	returnImmediately bool
	closeErr          error
	closeCalls        int
	operations        []string
}

func newRuntimeTestGenerator() *runtimeTestGenerator {
	return &runtimeTestGenerator{refreshStarted: make(chan struct{})}
}

func (g *runtimeTestGenerator) RunRefreshLoop(ctx context.Context) error {
	g.record("refresh-start")
	close(g.refreshStarted)
	var err error
	if g.returnImmediately {
		err = g.refreshErr
	} else if g.failRefresh != nil {
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

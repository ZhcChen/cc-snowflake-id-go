package lease

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrRefreshLoopStopped 表示 refresh loop 非预期退出。
var ErrRefreshLoopStopped = errors.New("idgen: refresh loop stopped unexpectedly")

// RuntimeGenerator 定义 Runtime 管理的最小生成器生命周期接口。
type RuntimeGenerator interface {
	// RunRefreshLoop 在给定上下文下运行后台刷新循环。
	RunRefreshLoop(context.Context) error
	// Close 释放生成器持有的外部资源。
	Close(context.Context) error
}

// Runtime 持有一个生成器的 refresh goroutine，并统一管理关闭顺序。
type Runtime struct {
	ctx       context.Context
	cancel    context.CancelCauseFunc
	generator RuntimeGenerator
	done      chan struct{}

	errMu sync.Mutex
	err   error

	stopOnce sync.Once
	stopDone chan struct{}
	stopErr  error
}

// StartRuntime 启动一个受上下文管理的生成器运行时。
func StartRuntime(parent context.Context, generator RuntimeGenerator) (*Runtime, error) {
	if generator == nil {
		return nil, fmt.Errorf("%w: runtime generator is required", ErrInvalidLeaseConfig)
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancelCause(parent)
	runtime := &Runtime{
		ctx:       ctx,
		cancel:    cancel,
		generator: generator,
		done:      make(chan struct{}),
		stopDone:  make(chan struct{}),
	}
	go runtime.run()
	return runtime, nil
}

// Context 返回 Runtime 内部使用的上下文。
func (r *Runtime) Context() context.Context {
	if r == nil || r.ctx == nil {
		return context.Background()
	}
	return r.ctx
}

// Done 在 refresh loop 结束时关闭。
func (r *Runtime) Done() <-chan struct{} {
	if r == nil || r.done == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	return r.done
}

// Err 返回 Runtime 记录到的首个终止错误。
func (r *Runtime) Err() error {
	if r == nil {
		return nil
	}
	r.errMu.Lock()
	defer r.errMu.Unlock()
	return r.err
}

// Stop 按 cancel、等待退出、再 Close 的顺序关闭运行时。
func (r *Runtime) Stop(closeCtx context.Context) error {
	if r == nil {
		return nil
	}
	if closeCtx == nil {
		closeCtx = context.Background()
	}
	r.stopOnce.Do(func() {
		// 先让 refresh loop 退出，再执行 Close，避免关闭顺序反转。
		r.cancel(context.Canceled)
		<-r.done
		closeErr := r.generator.Close(closeCtx)
		r.stopErr = errors.Join(r.Err(), closeErr)
		close(r.stopDone)
	})
	<-r.stopDone
	return r.stopErr
}

func (r *Runtime) run() {
	err := r.generator.RunRefreshLoop(r.ctx)
	if r.ctx.Err() == nil {
		if err == nil {
			err = ErrRefreshLoopStopped
		}
		r.recordError(err)
	} else if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		r.recordError(err)
	}
	close(r.done)
}

func (r *Runtime) recordError(err error) {
	if err == nil {
		return
	}
	r.errMu.Lock()
	if r.err == nil {
		r.err = err
		r.cancel(err)
	}
	r.errMu.Unlock()
}

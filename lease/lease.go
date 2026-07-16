package lease

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	sf "github.com/ZhcChen/cc-snowflake-id-go/generator"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	// ErrInvalidLeaseConfig 表示租约或租约发号配置不合法。
	ErrInvalidLeaseConfig = errors.New("idgen: invalid lease config")
	// ErrLeaseUnavailable 表示当前节点租约暂时被其他实例占用。
	ErrLeaseUnavailable = errors.New("idgen: node lease is unavailable")
	// ErrLeaseLost 表示当前进程不再持有租约。
	ErrLeaseLost = errors.New("idgen: node lease is no longer owned by this process")
	// ErrLeaseNotAcquired 表示调用方在未获取租约时尝试刷新或发号。
	ErrLeaseNotAcquired = errors.New("idgen: node lease has not been acquired")
	// ErrGenerationFenceAhead 表示持久化围栏已经领先于本地时钟。
	ErrGenerationFenceAhead = errors.New("idgen: persisted generation fence is ahead of local clock")
	// ErrClockSkew 表示应用与数据库时钟偏差超过允许范围。
	ErrClockSkew = errors.New("idgen: application and database clocks differ beyond the allowed skew")
	// ErrGeneratorClosed 表示租约生成器已经关闭。
	ErrGeneratorClosed = errors.New("idgen: leased generator is closed")
	// ErrLeaseAlreadyAcquired 表示同一个 LeaseManager 被重复 acquire。
	ErrLeaseAlreadyAcquired = errors.New("idgen: node lease is already acquired")
	// ErrLeaseStore 表示底层租约存储操作失败。
	ErrLeaseStore = errors.New("idgen: lease store operation failed")
)

const (
	leaseAcquireRetryDelayMillis int64 = 1
	// DefaultFenceWindow 是默认的 generation fence 窗口。
	DefaultFenceWindow = 5 * time.Second
	// DefaultMaxClockSkew 是默认允许的应用与数据库时钟偏差。
	DefaultMaxClockSkew = time.Second
	// DefaultLeaseOperationTimeout 是单次租约存储操作的默认超时。
	DefaultLeaseOperationTimeout = time.Second
)

// LeaseState 表示一次租约视图及其对应的发号边界。
type LeaseState struct {
	// NodeID 是租约对应的逻辑节点编号。
	NodeID int
	// OwnerID 是当前租约 owner 的原始标识。
	OwnerID string
	// ReservedUntilMillis 是数据库视角下的租约到期毫秒时间。
	ReservedUntilMillis int64
	// DatabaseNowMillis 是本次租约读写时数据库返回的当前毫秒时间。
	DatabaseNowMillis int64
	// GenerationFloorMillis 是当前实例允许发号的时间下界。
	GenerationFloorMillis int64
	// GenerationFenceMillis 是当前实例允许发号的时间上界。
	GenerationFenceMillis int64
}

// LeaseStore 定义租约获取、刷新和释放的持久化接口。
type LeaseStore interface {
	// TryAcquire 尝试获取租约，并返回当前租约状态和是否成功。
	TryAcquire(ctx context.Context, nodeID int, ownerID string, localNowMillis int64, leaseWindowMillis int64, generationFenceMillis int64, maxClockSkewMillis int64) (LeaseState, bool, error)
	// Refresh 延长当前 owner 持有的租约。
	Refresh(ctx context.Context, nodeID int, ownerID string, localNowMillis int64, leaseWindowMillis int64, generationFenceMillis int64, maxClockSkewMillis int64) (LeaseState, error)
	// Release 释放当前 owner 持有的租约。
	Release(ctx context.Context, nodeID int, ownerID string) error
}

// LeaseManagerConfig 描述租约协调器的配置。
type LeaseManagerConfig struct {
	// NodeID 是要争抢的逻辑节点编号。
	NodeID int
	// OwnerID 是当前进程实例的唯一 owner 标识。
	OwnerID string
	// LeaseWindow 是单次租约保留时长。
	LeaseWindow time.Duration
	// FenceWindow 是写入 generation fence 时使用的未来窗口。
	FenceWindow time.Duration
	// MaxClockSkew 是允许的应用与数据库时钟偏差上限。
	MaxClockSkew time.Duration
	// AcquireTimeout 是 acquire 阶段允许的总等待时长。
	AcquireTimeout time.Duration
	// OperationTimeout 是单次存储操作允许的最长时长。
	OperationTimeout time.Duration
	// Observer 用于采集租约生命周期事件和计数。
	Observer Observer
}

type leaseLifecycle uint8

const (
	leaseLifecycleNew leaseLifecycle = iota
	leaseLifecycleActive
	leaseLifecycleFailed
	leaseLifecycleClosed
)

// LeaseManager 负责单个 node_id 的租约生命周期管理。
type LeaseManager struct {
	mu sync.Mutex

	store    LeaseStore
	clock    sf.Clock
	config   LeaseManagerConfig
	observer Observer

	lifecycle                    leaseLifecycle
	state                        LeaseState
	leaseDeadlineMonotonicMillis int64
	leaseOwned                   bool
	terminalErr                  error
	closeErr                     error
	closeDone                    chan struct{}
}

// NewLeaseManager 创建一个只负责租约协调、不直接发号的管理器。
func NewLeaseManager(store LeaseStore, clock sf.Clock, cfg LeaseManagerConfig) (*LeaseManager, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: store is required", ErrInvalidLeaseConfig)
	}
	if clock == nil {
		clock = sf.SystemClock{}
	}
	if err := sf.ValidateNodeID(cfg.NodeID); err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.OwnerID) == "" {
		return nil, fmt.Errorf("%w: owner_id is required", ErrInvalidLeaseConfig)
	}
	if cfg.LeaseWindow < time.Millisecond {
		return nil, fmt.Errorf("%w: lease window must be at least 1ms", ErrInvalidLeaseConfig)
	}
	if cfg.FenceWindow == 0 {
		cfg.FenceWindow = cfg.LeaseWindow
	}
	if cfg.FenceWindow < time.Millisecond {
		return nil, fmt.Errorf("%w: fence window must be at least 1ms", ErrInvalidLeaseConfig)
	}
	if cfg.MaxClockSkew == 0 {
		cfg.MaxClockSkew = DefaultMaxClockSkew
	}
	if cfg.MaxClockSkew < time.Millisecond {
		return nil, fmt.Errorf("%w: max clock skew must be at least 1ms", ErrInvalidLeaseConfig)
	}
	if cfg.AcquireTimeout < 0 {
		return nil, fmt.Errorf("%w: acquire timeout must be non-negative", ErrInvalidLeaseConfig)
	}
	if cfg.OperationTimeout < 0 {
		return nil, fmt.Errorf("%w: operation timeout must be non-negative", ErrInvalidLeaseConfig)
	}
	if cfg.OperationTimeout == 0 {
		cfg.OperationTimeout = DefaultLeaseOperationTimeout
	}
	cfg.OwnerID = strings.TrimSpace(cfg.OwnerID)
	return &LeaseManager{store: store, clock: clock, config: cfg, observer: cfg.Observer}, nil
}

// Acquire 申请并激活当前节点的租约。
func (m *LeaseManager) Acquire(ctx context.Context) (LeaseState, error) {
	if m == nil {
		return LeaseState{}, fmt.Errorf("%w: nil manager", ErrInvalidLeaseConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	switch m.lifecycle {
	case leaseLifecycleActive:
		return LeaseState{}, ErrLeaseAlreadyAcquired
	case leaseLifecycleFailed:
		return LeaseState{}, m.terminalErr
	case leaseLifecycleClosed:
		return LeaseState{}, ErrGeneratorClosed
	}

	state, err := m.acquireLocked(ctx)
	if err != nil {
		return LeaseState{}, m.failLocked(err)
	}
	return state, nil
}

func (m *LeaseManager) acquireLocked(ctx context.Context) (LeaseState, error) {
	deadlineMonotonicMillis := m.clock.MonotonicMillis() + m.config.AcquireTimeout.Milliseconds()
	for {
		localNowMillis := m.clock.NowMillis()
		generationFenceMillis, err := addDurationMillis(localNowMillis, m.config.FenceWindow)
		if err != nil {
			return LeaseState{}, err
		}
		requestMonotonicMillis := m.clock.MonotonicMillis()
		operationCtx, cancel := m.operationContext(ctx)
		state, acquired, err := m.store.TryAcquire(
			operationCtx,
			m.config.NodeID,
			m.config.OwnerID,
			localNowMillis,
			m.config.LeaseWindow.Milliseconds(),
			generationFenceMillis,
			m.config.MaxClockSkew.Milliseconds(),
		)
		cancel()
		if err != nil {
			return LeaseState{}, wrapLeaseStoreError("acquire", err)
		}
		if acquired {
			state = normalizeLeaseState(state, localNowMillis, generationFenceMillis)
			state.GenerationFloorMillis = localNowMillis
			m.installStateLocked(state, requestMonotonicMillis)
			if m.observer != nil {
				m.observer.ObserveAcquired(m.state)
			}
			return m.state, nil
		}

		state = normalizeLeaseState(state, localNowMillis, localNowMillis)
		waitMillis, waitErr := acquireWait(state, localNowMillis)
		responseMonotonicMillis := m.clock.MonotonicMillis()
		waitMillis = deductAcquireRoundTrip(waitMillis, responseMonotonicMillis-requestMonotonicMillis)
		remainingMillis := deadlineMonotonicMillis - responseMonotonicMillis
		if remainingMillis <= 0 || waitMillis > remainingMillis {
			return LeaseState{}, acquireTimeoutError(waitErr, state, localNowMillis, remainingMillis)
		}
		if err := m.clock.Sleep(ctx, time.Duration(waitMillis)*time.Millisecond); err != nil {
			return LeaseState{}, err
		}
	}
}

func acquireWait(state LeaseState, localNowMillis int64) (int64, error) {
	if state.ReservedUntilMillis > state.DatabaseNowMillis {
		return state.ReservedUntilMillis - state.DatabaseNowMillis, ErrLeaseUnavailable
	}
	if state.GenerationFenceMillis > localNowMillis {
		return state.GenerationFenceMillis - localNowMillis, ErrGenerationFenceAhead
	}
	return leaseAcquireRetryDelayMillis, ErrLeaseUnavailable
}

func deductAcquireRoundTrip(waitMillis int64, elapsedMillis int64) int64 {
	if elapsedMillis <= 0 {
		return waitMillis
	}
	if elapsedMillis >= waitMillis {
		return leaseAcquireRetryDelayMillis
	}
	return waitMillis - elapsedMillis
}

func acquireTimeoutError(reason error, state LeaseState, localNowMillis int64, remainingMillis int64) error {
	return fmt.Errorf(
		"%w: node_id=%d owner_id=%q db_now_ms=%d reserved_until_ms=%d local_now_ms=%d generation_fence_ms=%d timeout_remaining_ms=%d",
		reason,
		state.NodeID,
		RedactOwnerID(state.OwnerID),
		state.DatabaseNowMillis,
		state.ReservedUntilMillis,
		localNowMillis,
		state.GenerationFenceMillis,
		remainingMillis,
	)
}

// Refresh 延长当前租约，并更新 generation fence。
func (m *LeaseManager) Refresh(ctx context.Context) (LeaseState, error) {
	if m == nil {
		return LeaseState{}, fmt.Errorf("%w: nil manager", ErrInvalidLeaseConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.refreshLocked(ctx)
}

func (m *LeaseManager) refreshLocked(ctx context.Context) (LeaseState, error) {
	if err := m.ensureActiveLocked(); err != nil {
		return LeaseState{}, err
	}
	localNowMillis := m.clock.NowMillis()
	generationFenceMillis, err := addDurationMillis(localNowMillis, m.config.FenceWindow)
	if err != nil {
		if m.observer != nil {
			m.observer.ObserveRefresh(false)
		}
		return LeaseState{}, m.failLocked(err)
	}
	requestMonotonicMillis := m.clock.MonotonicMillis()
	operationCtx, cancel := m.operationContext(ctx)
	state, err := m.store.Refresh(
		operationCtx,
		m.config.NodeID,
		m.config.OwnerID,
		localNowMillis,
		m.config.LeaseWindow.Milliseconds(),
		generationFenceMillis,
		m.config.MaxClockSkew.Milliseconds(),
	)
	cancel()
	if err != nil {
		if m.observer != nil {
			m.observer.ObserveRefresh(false)
		}
		return LeaseState{}, m.failLocked(wrapLeaseStoreError("refresh", err))
	}
	state = normalizeLeaseState(state, localNowMillis, generationFenceMillis)
	state.GenerationFloorMillis = m.state.GenerationFloorMillis
	m.installStateLocked(state, requestMonotonicMillis)
	if m.observer != nil {
		m.observer.ObserveRefresh(true)
	}
	return m.state, nil
}

// Close 关闭租约管理器，并在仍持有租约时尝试释放。
func (m *LeaseManager) Close(ctx context.Context) error {
	if m == nil {
		return fmt.Errorf("%w: nil manager", ErrInvalidLeaseConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.Lock()
	if m.lifecycle == leaseLifecycleClosed {
		closeDone := m.closeDone
		m.mu.Unlock()
		if closeDone != nil {
			<-closeDone
		}
		m.mu.Lock()
		closeErr := m.closeErr
		m.mu.Unlock()
		return closeErr
	}

	shouldRelease := m.leaseOwned
	m.lifecycle = leaseLifecycleClosed
	m.terminalErr = ErrGeneratorClosed
	m.leaseDeadlineMonotonicMillis = 0
	m.closeDone = make(chan struct{})
	closeDone := m.closeDone
	m.mu.Unlock()

	var closeErr error
	if shouldRelease {
		operationCtx, cancel := m.operationContext(ctx)
		closeErr = wrapLeaseStoreError("release", m.store.Release(operationCtx, m.config.NodeID, m.config.OwnerID))
		cancel()
	}

	m.mu.Lock()
	m.leaseOwned = false
	m.closeErr = closeErr
	if m.observer != nil {
		m.observer.ObserveClosed(m.observerStateLocked(), closeErr)
	}
	close(closeDone)
	m.mu.Unlock()
	return closeErr
}

// Release 是 Close 的别名，保留给更贴近租约语义的调用方。
func (m *LeaseManager) Release(ctx context.Context) error {
	return m.Close(ctx)
}

func (m *LeaseManager) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if m.config.OperationTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, m.config.OperationTimeout)
}

// State 返回当前管理器持有的最新租约状态快照。
func (m *LeaseManager) State() LeaseState {
	if m == nil {
		return LeaseState{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

func (m *LeaseManager) ensureActiveLocked() error {
	switch m.lifecycle {
	case leaseLifecycleNew:
		return ErrLeaseNotAcquired
	case leaseLifecycleFailed:
		return m.terminalErr
	case leaseLifecycleClosed:
		return ErrGeneratorClosed
	}
	if m.clock.MonotonicMillis() >= m.leaseDeadlineMonotonicMillis {
		err := fmt.Errorf("%w: node_id=%d reserved_until_ms=%d db_now_ms=%d", ErrLeaseLost, m.config.NodeID, m.state.ReservedUntilMillis, m.state.DatabaseNowMillis)
		return m.failLocked(err)
	}
	return nil
}

func (m *LeaseManager) leaseRemainingMillisLocked() int64 {
	if m.lifecycle != leaseLifecycleActive {
		return 0
	}
	remainingMillis := m.leaseDeadlineMonotonicMillis - m.clock.MonotonicMillis()
	if remainingMillis < 0 {
		return 0
	}
	return remainingMillis
}

func (m *LeaseManager) installStateLocked(state LeaseState, requestMonotonicMillis int64) {
	remainingMillis := state.ReservedUntilMillis - state.DatabaseNowMillis
	if remainingMillis < 0 {
		remainingMillis = 0
	}
	m.state = state
	m.lifecycle = leaseLifecycleActive
	m.leaseOwned = true
	m.leaseDeadlineMonotonicMillis = requestMonotonicMillis + remainingMillis
}

func (m *LeaseManager) failLocked(err error) error {
	if m.lifecycle == leaseLifecycleClosed {
		return ErrGeneratorClosed
	}
	if m.lifecycle == leaseLifecycleFailed && m.terminalErr != nil {
		return m.terminalErr
	}
	if err == nil {
		err = ErrLeaseLost
	}
	m.lifecycle = leaseLifecycleFailed
	m.terminalErr = err
	if m.observer != nil {
		m.observer.ObserveFailed(m.observerStateLocked(), err)
	}
	return err
}

func (m *LeaseManager) observerStateLocked() LeaseState {
	state := m.state
	if state.NodeID == 0 {
		state.NodeID = m.config.NodeID
		state.OwnerID = m.config.OwnerID
	}
	return state
}

func normalizeLeaseState(state LeaseState, localNowMillis int64, fallbackFenceMillis int64) LeaseState {
	if state.DatabaseNowMillis <= 0 {
		state.DatabaseNowMillis = localNowMillis
	}
	if state.GenerationFenceMillis <= 0 {
		state.GenerationFenceMillis = fallbackFenceMillis
	}
	return state
}

// LeasedGeneratorConfig 描述带租约保护的雪花发号器配置。
type LeasedGeneratorConfig struct {
	// NodeID 是当前发号器使用的逻辑节点编号。
	NodeID int
	// OwnerID 是当前实例的唯一 owner 标识。
	OwnerID string
	// EpochMillis 是雪花 ID 使用的纪元起点。
	EpochMillis int64
	// SmallRollbackWait 定义可容忍的小幅时钟回退等待时间。
	SmallRollbackWait time.Duration
	// LeaseWindow 是单次租约保留时长。
	LeaseWindow time.Duration
	// FenceWindow 是写入 generation fence 时使用的未来窗口。
	FenceWindow time.Duration
	// MaxClockSkew 是允许的应用与数据库时钟偏差上限。
	MaxClockSkew time.Duration
	// LeaseAcquireTimeout 是 acquire 阶段允许的总等待时长。
	LeaseAcquireTimeout time.Duration
	// LeaseOperationTimeout 是单次租约存储操作允许的最长时长。
	LeaseOperationTimeout time.Duration
	// LeaseRefreshInterval 是后台 refresh loop 的刷新间隔。
	LeaseRefreshInterval time.Duration
	// Observer 用于采集发号与租约生命周期事件和计数。
	Observer Observer
}

// LeasedGenerator 把进程内发号器和租约管理器组合为可多实例安全使用的发号器。
type LeasedGenerator struct {
	generator        *sf.Generator
	leaseManager     *LeaseManager
	clock            sf.Clock
	refreshInterval  time.Duration
	operationTimeout time.Duration
	observer         Observer
}

// NewLeasedGenerator 创建一个带数据库租约保护的雪花发号器。
func NewLeasedGenerator(store LeaseStore, clock sf.Clock, cfg LeasedGeneratorConfig) (*LeasedGenerator, error) {
	if clock == nil {
		clock = sf.SystemClock{}
	}
	if cfg.LeaseRefreshInterval <= 0 {
		return nil, fmt.Errorf("%w: lease refresh interval must be greater than zero", ErrInvalidLeaseConfig)
	}
	if cfg.LeaseWindow <= cfg.LeaseRefreshInterval {
		return nil, fmt.Errorf("%w: lease window must be greater than refresh interval", ErrInvalidLeaseConfig)
	}
	if cfg.FenceWindow == 0 {
		cfg.FenceWindow = cfg.LeaseWindow
	}
	if cfg.FenceWindow <= cfg.LeaseRefreshInterval {
		return nil, fmt.Errorf("%w: fence window must be greater than refresh interval", ErrInvalidLeaseConfig)
	}
	if cfg.MaxClockSkew == 0 {
		cfg.MaxClockSkew = DefaultMaxClockSkew
	}
	if cfg.MaxClockSkew < time.Millisecond {
		return nil, fmt.Errorf("%w: max clock skew must be at least 1ms", ErrInvalidLeaseConfig)
	}
	if cfg.LeaseOperationTimeout < 0 {
		return nil, fmt.Errorf("%w: lease operation timeout must be non-negative", ErrInvalidLeaseConfig)
	}
	if cfg.LeaseOperationTimeout == 0 {
		cfg.LeaseOperationTimeout = DefaultLeaseOperationTimeout
	}
	if cfg.LeaseWindow-cfg.LeaseRefreshInterval <= cfg.LeaseOperationTimeout {
		return nil, fmt.Errorf("%w: lease window must be greater than refresh interval plus operation timeout", ErrInvalidLeaseConfig)
	}
	if cfg.FenceWindow-cfg.LeaseRefreshInterval <= cfg.LeaseOperationTimeout {
		return nil, fmt.Errorf("%w: fence window must be greater than refresh interval plus operation timeout", ErrInvalidLeaseConfig)
	}
	generator, err := sf.NewGenerator(sf.Config{
		NodeID:            cfg.NodeID,
		EpochMillis:       cfg.EpochMillis,
		SmallRollbackWait: cfg.SmallRollbackWait,
	}, clock)
	if err != nil {
		return nil, err
	}
	leaseManager, err := NewLeaseManager(store, clock, LeaseManagerConfig{
		NodeID:           cfg.NodeID,
		OwnerID:          cfg.OwnerID,
		LeaseWindow:      cfg.LeaseWindow,
		FenceWindow:      cfg.FenceWindow,
		MaxClockSkew:     cfg.MaxClockSkew,
		AcquireTimeout:   cfg.LeaseAcquireTimeout,
		OperationTimeout: cfg.LeaseOperationTimeout,
		Observer:         cfg.Observer,
	})
	if err != nil {
		return nil, err
	}
	return &LeasedGenerator{
		generator:        generator,
		leaseManager:     leaseManager,
		clock:            clock,
		refreshInterval:  cfg.LeaseRefreshInterval,
		operationTimeout: cfg.LeaseOperationTimeout,
		observer:         cfg.Observer,
	}, nil
}

// Acquire 获取发号所需的节点租约。
func (g *LeasedGenerator) Acquire(ctx context.Context) (LeaseState, error) {
	if g == nil {
		return LeaseState{}, fmt.Errorf("%w: nil leased generator", ErrInvalidLeaseConfig)
	}
	return g.leaseManager.Acquire(ctx)
}

// Next 在当前租约和 generation fence 允许的范围内生成一个新 ID。
func (g *LeasedGenerator) Next(ctx context.Context) (int64, error) {
	if g == nil {
		return 0, fmt.Errorf("%w: nil leased generator", ErrInvalidLeaseConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m := g.leaseManager
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := g.ensureReadyLocked(ctx); err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	id, err := g.generator.NextWithin(ctx, m.state.GenerationFloorMillis, m.state.GenerationFenceMillis)
	if errors.Is(err, sf.ErrGenerationFenceReached) {
		if _, refreshErr := g.refreshForNextLocked(ctx); refreshErr != nil {
			return 0, refreshErr
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		id, err = g.generator.NextWithin(ctx, m.state.GenerationFloorMillis, m.state.GenerationFenceMillis)
	}
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		err = m.failLocked(err)
	}
	if err == nil && g.observer != nil {
		g.observer.ObserveGenerated()
	}
	return id, err
}

// Refresh 主动刷新租约和 generation fence。
func (g *LeasedGenerator) Refresh(ctx context.Context) (LeaseState, error) {
	if g == nil {
		return LeaseState{}, fmt.Errorf("%w: nil leased generator", ErrInvalidLeaseConfig)
	}
	return g.leaseManager.Refresh(ctx)
}

// RunRefreshLoop 按固定周期持续刷新租约，直到上下文结束或刷新失败。
func (g *LeasedGenerator) RunRefreshLoop(ctx context.Context) error {
	if g == nil {
		return fmt.Errorf("%w: nil leased generator", ErrInvalidLeaseConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(g.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := ctx.Err(); err != nil {
				return err
			}
			refreshCtx, cancel := g.leaseOperationContext(ctx)
			_, err := g.Refresh(refreshCtx)
			cancel()
			if err != nil {
				return err
			}
		}
	}
}

// Close 停止租约生成器，并在可能时释放租约。
func (g *LeasedGenerator) Close(ctx context.Context) error {
	if g == nil {
		return fmt.Errorf("%w: nil leased generator", ErrInvalidLeaseConfig)
	}
	return g.leaseManager.Close(ctx)
}

// Release 是 Close 的别名，保留给更贴近租约语义的调用方。
func (g *LeasedGenerator) Release(ctx context.Context) error {
	return g.Close(ctx)
}

// State 返回当前租约生成器看到的最新租约状态。
func (g *LeasedGenerator) State() LeaseState {
	if g == nil {
		return LeaseState{}
	}
	return g.leaseManager.State()
}

func (g *LeasedGenerator) ensureReadyLocked(ctx context.Context) error {
	m := g.leaseManager
	if err := m.ensureActiveLocked(); err != nil {
		return err
	}
	nowMillis := g.clock.NowMillis()
	if nowMillis < m.state.GenerationFloorMillis {
		err := fmt.Errorf("%w: node_id=%d now_ms=%d floor_ms=%d", sf.ErrTimestampBelowGenerationFloor, m.state.NodeID, nowMillis, m.state.GenerationFloorMillis)
		return m.failLocked(err)
	}
	refreshLeadMillis := g.refreshInterval.Milliseconds()
	if m.leaseRemainingMillisLocked() <= refreshLeadMillis ||
		m.state.GenerationFenceMillis-nowMillis <= refreshLeadMillis {
		_, err := g.refreshForNextLocked(ctx)
		return err
	}
	return nil
}

func (g *LeasedGenerator) refreshForNextLocked(ctx context.Context) (LeaseState, error) {
	if err := ctx.Err(); err != nil {
		return LeaseState{}, err
	}
	refreshCtx, cancel := g.leaseOperationContext(ctx)
	defer cancel()
	state, err := g.leaseManager.refreshLocked(refreshCtx)
	if err != nil {
		return LeaseState{}, err
	}
	if err := ctx.Err(); err != nil {
		return LeaseState{}, err
	}
	return state, nil
}

func (g *LeasedGenerator) leaseOperationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	base := context.WithoutCancel(ctx)
	if g.operationTimeout <= 0 {
		return base, func() {}
	}
	return context.WithTimeout(base, g.operationTimeout)
}

// PGLeaseStore 是基于 PostgreSQL 表实现的租约存储。
type PGLeaseStore struct {
	db LeaseDB
}

// LeaseDB 定义 PGLeaseStore 依赖的最小数据库能力。
type LeaseDB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// NewPGLeaseStore 创建一个 PostgreSQL 租约存储实现。
func NewPGLeaseStore(db LeaseDB) (*PGLeaseStore, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: db is required", ErrInvalidLeaseConfig)
	}
	return &PGLeaseStore{db: db}, nil
}

// TryAcquire 尝试为指定 node_id 获取租约。
func (s *PGLeaseStore) TryAcquire(
	ctx context.Context,
	nodeID int,
	ownerID string,
	localNowMillis int64,
	leaseWindowMillis int64,
	generationFenceMillis int64,
	maxClockSkewMillis int64,
) (LeaseState, bool, error) {
	if err := validateStoreIdentityRequest(nodeID, ownerID); err != nil {
		return LeaseState{}, false, err
	}
	if err := validateStoreLeaseRequest(localNowMillis, leaseWindowMillis, generationFenceMillis, maxClockSkewMillis); err != nil {
		return LeaseState{}, false, err
	}

	var (
		acquired bool
		state    LeaseState
		outcome  string
	)
	err := s.db.QueryRow(ctx, `
WITH db_clock AS (
    SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT AS now_ms
),
candidate AS (
    SELECT now_ms,
           ABS($3::BIGINT - now_ms) <= $6::BIGINT AS clock_ok
    FROM db_clock
),
upsert AS (
    INSERT INTO id_generator_node_leases (
        node_id,
        owner_id,
        reserved_until_ms,
        generation_fence_ms,
        acquired_at_ms,
        refreshed_at_ms,
        heartbeat_at_ms,
        lease_version,
        created_at,
        updated_at
    )
    SELECT $1,
           $2,
           candidate.now_ms + $4,
           $5,
           candidate.now_ms,
           candidate.now_ms,
           candidate.now_ms,
           1,
           NOW(),
           NOW()
    FROM candidate
    WHERE candidate.clock_ok
    ON CONFLICT (node_id) DO UPDATE
    SET owner_id = EXCLUDED.owner_id,
        reserved_until_ms = EXCLUDED.reserved_until_ms,
        generation_fence_ms = GREATEST(
            id_generator_node_leases.generation_fence_ms,
            EXCLUDED.generation_fence_ms
        ),
        acquired_at_ms = CASE
            WHEN id_generator_node_leases.owner_id = EXCLUDED.owner_id THEN id_generator_node_leases.acquired_at_ms
            ELSE EXCLUDED.acquired_at_ms
        END,
        refreshed_at_ms = EXCLUDED.refreshed_at_ms,
        heartbeat_at_ms = EXCLUDED.heartbeat_at_ms,
        lease_version = id_generator_node_leases.lease_version + 1,
        updated_at = NOW()
    WHERE id_generator_node_leases.owner_id = EXCLUDED.owner_id
       OR (
            id_generator_node_leases.reserved_until_ms <= (SELECT now_ms FROM candidate)
        AND $3::BIGINT >= id_generator_node_leases.generation_fence_ms
       )
    RETURNING node_id, owner_id, reserved_until_ms, generation_fence_ms
)
SELECT TRUE AS acquired,
       node_id,
       owner_id,
       reserved_until_ms,
       generation_fence_ms,
       (SELECT now_ms FROM candidate) AS db_now_ms,
       'acquired'::TEXT AS outcome
FROM upsert
UNION ALL
SELECT FALSE AS acquired,
       COALESCE(existing.node_id, $1::INTEGER),
       COALESCE(existing.owner_id, ''),
       COALESCE(existing.reserved_until_ms, 0),
       COALESCE(existing.generation_fence_ms, 0),
       candidate.now_ms,
       CASE
           WHEN NOT candidate.clock_ok THEN 'clock_skew'
           WHEN existing.reserved_until_ms > candidate.now_ms THEN 'lease_busy'
           WHEN $3::BIGINT < existing.generation_fence_ms THEN 'fence_ahead'
           ELSE 'retry'
       END
FROM candidate
LEFT JOIN id_generator_node_leases AS existing ON existing.node_id = $1
WHERE NOT EXISTS (SELECT 1 FROM upsert)
LIMIT 1`,
		nodeID,
		ownerID,
		localNowMillis,
		leaseWindowMillis,
		generationFenceMillis,
		maxClockSkewMillis,
	).Scan(
		&acquired,
		&state.NodeID,
		&state.OwnerID,
		&state.ReservedUntilMillis,
		&state.GenerationFenceMillis,
		&state.DatabaseNowMillis,
		&outcome,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return LeaseState{
				NodeID:                nodeID,
				DatabaseNowMillis:     localNowMillis,
				GenerationFenceMillis: generationFenceMillis,
			}, false, nil
		}
		return LeaseState{}, false, err
	}
	if outcome == "clock_skew" {
		return state, false, clockSkewError(localNowMillis, state.DatabaseNowMillis, maxClockSkewMillis)
	}
	return state, acquired, nil
}

// Refresh 刷新当前 owner 持有的租约。
func (s *PGLeaseStore) Refresh(
	ctx context.Context,
	nodeID int,
	ownerID string,
	localNowMillis int64,
	leaseWindowMillis int64,
	generationFenceMillis int64,
	maxClockSkewMillis int64,
) (LeaseState, error) {
	if err := validateStoreIdentityRequest(nodeID, ownerID); err != nil {
		return LeaseState{}, err
	}
	if err := validateStoreLeaseRequest(localNowMillis, leaseWindowMillis, generationFenceMillis, maxClockSkewMillis); err != nil {
		return LeaseState{}, err
	}

	var (
		refreshed bool
		state     LeaseState
		outcome   string
	)
	err := s.db.QueryRow(ctx, `
WITH db_clock AS (
    SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT AS now_ms
),
candidate AS (
    SELECT now_ms,
           ABS($3::BIGINT - now_ms) <= $6::BIGINT AS clock_ok
    FROM db_clock
),
updated AS (
    UPDATE id_generator_node_leases
    SET reserved_until_ms = (SELECT now_ms FROM candidate) + $4,
        generation_fence_ms = GREATEST(generation_fence_ms, $5::BIGINT),
        refreshed_at_ms = (SELECT now_ms FROM candidate),
        heartbeat_at_ms = (SELECT now_ms FROM candidate),
        lease_version = lease_version + 1,
        updated_at = NOW()
    WHERE node_id = $1
      AND owner_id = $2
      AND reserved_until_ms > (SELECT now_ms FROM candidate)
      AND (SELECT clock_ok FROM candidate)
    RETURNING node_id, owner_id, reserved_until_ms, generation_fence_ms
)
SELECT TRUE AS refreshed,
       node_id,
       owner_id,
       reserved_until_ms,
       generation_fence_ms,
       (SELECT now_ms FROM candidate) AS db_now_ms,
       'refreshed'::TEXT AS outcome
FROM updated
UNION ALL
SELECT FALSE AS refreshed,
       COALESCE(existing.node_id, $1::INTEGER),
       COALESCE(existing.owner_id, ''),
       COALESCE(existing.reserved_until_ms, 0),
       COALESCE(existing.generation_fence_ms, 0),
       candidate.now_ms,
       CASE
           WHEN NOT candidate.clock_ok THEN 'clock_skew'
           ELSE 'lease_lost'
       END
FROM candidate
LEFT JOIN id_generator_node_leases AS existing ON existing.node_id = $1
WHERE NOT EXISTS (SELECT 1 FROM updated)
LIMIT 1`,
		nodeID,
		ownerID,
		localNowMillis,
		leaseWindowMillis,
		generationFenceMillis,
		maxClockSkewMillis,
	).Scan(
		&refreshed,
		&state.NodeID,
		&state.OwnerID,
		&state.ReservedUntilMillis,
		&state.GenerationFenceMillis,
		&state.DatabaseNowMillis,
		&outcome,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return LeaseState{}, ErrLeaseLost
		}
		return LeaseState{}, err
	}
	if outcome == "clock_skew" {
		return LeaseState{}, clockSkewError(localNowMillis, state.DatabaseNowMillis, maxClockSkewMillis)
	}
	if !refreshed {
		return LeaseState{}, ErrLeaseLost
	}
	return state, nil
}

// Release 释放当前 owner 持有的租约。
func (s *PGLeaseStore) Release(ctx context.Context, nodeID int, ownerID string) error {
	if err := validateStoreIdentityRequest(nodeID, ownerID); err != nil {
		return err
	}
	_, err := s.db.Exec(ctx, `
UPDATE id_generator_node_leases
SET updated_at = NOW()
WHERE node_id = $1
  AND owner_id = $2`, nodeID, ownerID)
	return err
}

func validateStoreIdentityRequest(nodeID int, ownerID string) error {
	if err := sf.ValidateNodeID(nodeID); err != nil {
		return err
	}
	trimmedOwnerID := strings.TrimSpace(ownerID)
	if trimmedOwnerID == "" {
		return fmt.Errorf("%w: owner_id is required", ErrInvalidLeaseConfig)
	}
	if trimmedOwnerID != ownerID {
		return fmt.Errorf("%w: owner_id must not contain leading or trailing whitespace", ErrInvalidLeaseConfig)
	}
	return nil
}

func validateStoreLeaseRequest(localNowMillis int64, leaseWindowMillis int64, generationFenceMillis int64, maxClockSkewMillis int64) error {
	if localNowMillis <= 0 {
		return fmt.Errorf("%w: local_now_ms must be positive", ErrInvalidLeaseConfig)
	}
	if leaseWindowMillis <= 0 {
		return fmt.Errorf("%w: lease_window_ms must be positive", ErrInvalidLeaseConfig)
	}
	if generationFenceMillis <= localNowMillis {
		return fmt.Errorf("%w: generation_fence_ms must be greater than local_now_ms", ErrInvalidLeaseConfig)
	}
	if maxClockSkewMillis <= 0 {
		return fmt.Errorf("%w: max_clock_skew_ms must be positive", ErrInvalidLeaseConfig)
	}
	return nil
}

func addDurationMillis(nowMillis int64, duration time.Duration) (int64, error) {
	windowMillis := duration.Milliseconds()
	if windowMillis <= 0 || nowMillis > math.MaxInt64-windowMillis {
		return 0, fmt.Errorf("%w: timestamp window overflows milliseconds", ErrInvalidLeaseConfig)
	}
	return nowMillis + windowMillis, nil
}

func clockSkewError(localNowMillis int64, databaseNowMillis int64, maxClockSkewMillis int64) error {
	return fmt.Errorf(
		"%w: local_now_ms=%d db_now_ms=%d max_clock_skew_ms=%d",
		ErrClockSkew,
		localNowMillis,
		databaseNowMillis,
		maxClockSkewMillis,
	)
}

func wrapLeaseStoreError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %s: %w", ErrLeaseStore, operation, err)
}

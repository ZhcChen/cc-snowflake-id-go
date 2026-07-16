package lease

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	sf "github.com/ZhcChen/cc-snowflake-id-go/generator"
)

func TestLeaseConfigurationRejectsUnsafeTimingWindows(t *testing.T) {
	store := &fakeLeaseStore{}
	managerTests := []struct {
		name   string
		change func(*LeaseManagerConfig)
	}{
		{name: "lease below millisecond", change: func(cfg *LeaseManagerConfig) { cfg.LeaseWindow = time.Nanosecond }},
		{name: "fence below millisecond", change: func(cfg *LeaseManagerConfig) { cfg.FenceWindow = time.Nanosecond }},
		{name: "skew below millisecond", change: func(cfg *LeaseManagerConfig) { cfg.MaxClockSkew = time.Nanosecond }},
		{name: "negative acquire timeout", change: func(cfg *LeaseManagerConfig) { cfg.AcquireTimeout = -time.Millisecond }},
		{name: "negative operation timeout", change: func(cfg *LeaseManagerConfig) { cfg.OperationTimeout = -time.Millisecond }},
	}
	for _, test := range managerTests {
		t.Run("manager "+test.name, func(t *testing.T) {
			cfg := LeaseManagerConfig{
				NodeID:         7,
				OwnerID:        " owner-a ",
				LeaseWindow:    time.Second,
				FenceWindow:    time.Second,
				MaxClockSkew:   time.Second,
				AcquireTimeout: time.Second,
			}
			test.change(&cfg)
			if _, err := NewLeaseManager(store, nil, cfg); !errors.Is(err, ErrInvalidLeaseConfig) {
				t.Fatalf("NewLeaseManager() error = %v, want ErrInvalidLeaseConfig", err)
			}
		})
	}
	manager, err := NewLeaseManager(store, nil, LeaseManagerConfig{
		NodeID:      7,
		OwnerID:     " owner-a ",
		LeaseWindow: time.Second,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager(default windows) error = %v", err)
	}
	if manager.config.OwnerID != "owner-a" || manager.config.FenceWindow != time.Second || manager.config.MaxClockSkew != DefaultMaxClockSkew {
		t.Fatalf("normalized manager config = %+v", manager.config)
	}

	leasedTests := []struct {
		name   string
		change func(*LeasedGeneratorConfig)
	}{
		{name: "zero refresh interval", change: func(cfg *LeasedGeneratorConfig) { cfg.LeaseRefreshInterval = 0 }},
		{name: "lease not beyond refresh plus operation timeout", change: func(cfg *LeasedGeneratorConfig) { cfg.LeaseWindow = 1100 * time.Millisecond }},
		{name: "fence not beyond refresh plus operation timeout", change: func(cfg *LeasedGeneratorConfig) { cfg.FenceWindow = 1100 * time.Millisecond }},
		{name: "window margin sum overflows duration", change: func(cfg *LeasedGeneratorConfig) {
			maxDuration := time.Duration(1<<63 - 1)
			cfg.LeaseWindow = maxDuration
			cfg.FenceWindow = maxDuration
			cfg.LeaseRefreshInterval = maxDuration - time.Second
			cfg.LeaseOperationTimeout = 2 * time.Second
		}},
		{name: "skew below millisecond", change: func(cfg *LeasedGeneratorConfig) { cfg.MaxClockSkew = time.Nanosecond }},
		{name: "negative operation timeout", change: func(cfg *LeasedGeneratorConfig) { cfg.LeaseOperationTimeout = -time.Millisecond }},
	}
	for _, test := range leasedTests {
		t.Run("leased "+test.name, func(t *testing.T) {
			cfg := LeasedGeneratorConfig{
				NodeID:                7,
				OwnerID:               "owner-a",
				LeaseWindow:           3 * time.Second,
				FenceWindow:           3 * time.Second,
				MaxClockSkew:          time.Second,
				LeaseRefreshInterval:  time.Second,
				LeaseOperationTimeout: 100 * time.Millisecond,
			}
			test.change(&cfg)
			if _, err := NewLeasedGenerator(store, nil, cfg); !errors.Is(err, ErrInvalidLeaseConfig) {
				t.Fatalf("NewLeasedGenerator() error = %v, want ErrInvalidLeaseConfig", err)
			}
		})
	}
}

func TestPGLeaseRequestValidationRejectsInvalidTimeBounds(t *testing.T) {
	tests := []struct {
		name      string
		localNow  int64
		lease     int64
		fence     int64
		clockSkew int64
	}{
		{name: "non-positive local time", localNow: 0, lease: 1, fence: 2, clockSkew: 1},
		{name: "non-positive lease", localNow: 1, lease: 0, fence: 2, clockSkew: 1},
		{name: "fence not ahead", localNow: 2, lease: 1, fence: 2, clockSkew: 1},
		{name: "non-positive clock skew", localNow: 1, lease: 1, fence: 2, clockSkew: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateStoreLeaseRequest(test.localNow, test.lease, test.fence, test.clockSkew); !errors.Is(err, ErrInvalidLeaseConfig) {
				t.Fatalf("validateStoreLeaseRequest() error = %v, want ErrInvalidLeaseConfig", err)
			}
		})
	}
	if _, err := addDurationMillis(math.MaxInt64, time.Millisecond); !errors.Is(err, ErrInvalidLeaseConfig) {
		t.Fatalf("addDurationMillis(overflow) error = %v, want ErrInvalidLeaseConfig", err)
	}
	if _, err := addDurationMillis(1, time.Nanosecond); !errors.Is(err, ErrInvalidLeaseConfig) {
		t.Fatalf("addDurationMillis(sub-millisecond) error = %v, want ErrInvalidLeaseConfig", err)
	}
}

func TestPGLeaseRequestValidationRejectsInvalidIdentity(t *testing.T) {
	tests := []struct {
		name    string
		nodeID  int
		ownerID string
		wantErr error
	}{
		{name: "invalid node", nodeID: 0, ownerID: "owner-a", wantErr: sf.ErrInvalidNodeID},
		{name: "blank owner", nodeID: 7, ownerID: " ", wantErr: ErrInvalidLeaseConfig},
		{name: "untrimmed owner", nodeID: 7, ownerID: " owner-a ", wantErr: ErrInvalidLeaseConfig},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateStoreIdentityRequest(test.nodeID, test.ownerID); !errors.Is(err, test.wantErr) {
				t.Fatalf("validateStoreIdentityRequest() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestNilLeaseObjectsFailClosed(t *testing.T) {
	var manager *LeaseManager
	if _, err := manager.Acquire(nil); !errors.Is(err, ErrInvalidLeaseConfig) {
		t.Fatalf("nil manager Acquire() error = %v", err)
	}
	if _, err := manager.Refresh(nil); !errors.Is(err, ErrInvalidLeaseConfig) {
		t.Fatalf("nil manager Refresh() error = %v", err)
	}
	if err := manager.Close(nil); !errors.Is(err, ErrInvalidLeaseConfig) {
		t.Fatalf("nil manager Close() error = %v", err)
	}
	if state := manager.State(); state != (LeaseState{}) {
		t.Fatalf("nil manager State() = %+v", state)
	}

	var generator *LeasedGenerator
	operations := []struct {
		name string
		err  error
	}{
		{name: "Acquire", err: func() error { _, err := generator.Acquire(nil); return err }()},
		{name: "Next", err: func() error { _, err := generator.Next(nil); return err }()},
		{name: "Refresh", err: func() error { _, err := generator.Refresh(nil); return err }()},
		{name: "RunRefreshLoop", err: generator.RunRefreshLoop(nil)},
		{name: "Close", err: generator.Close(nil)},
		{name: "Release", err: generator.Release(nil)},
	}
	for _, operation := range operations {
		if !errors.Is(operation.err, ErrInvalidLeaseConfig) {
			t.Fatalf("nil generator %s error = %v, want ErrInvalidLeaseConfig", operation.name, operation.err)
		}
	}
	if state := generator.State(); state != (LeaseState{}) {
		t.Fatalf("nil generator State() = %+v", state)
	}
	if _, err := NewPGLeaseStore(nil); !errors.Is(err, ErrInvalidLeaseConfig) {
		t.Fatalf("NewPGLeaseStore(nil) error = %v, want ErrInvalidLeaseConfig", err)
	}
}

func TestValidationErrorMessagesRemainDiagnostic(t *testing.T) {
	err := validateStoreLeaseRequest(0, 0, 0, 0)
	if err == nil || !strings.Contains(err.Error(), "local_now_ms") {
		t.Fatalf("validation error = %v, want local_now_ms detail", err)
	}
}

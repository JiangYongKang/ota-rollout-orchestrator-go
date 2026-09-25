// Package orchestrator contains the rollout state machine: batched canary
// gating, idempotent/ordered report convergence, pause/terminate control and
// deterministic rollback isolation.
//
// Concurrency model: every state transition runs inside a single store.Lock
// transaction (the store serializes them). Commands to (simulated) devices
// are collected during the transaction and sent after the lock is released,
// so a slow/failing device never blocks or deadlocks state convergence.
package orchestrator

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/store"
)

// Commander is the device-facing port (fleet.Fleet satisfies it).
type Commander interface {
	Install(ctx context.Context, id, rolloutID string, seq int64, from, target string) error
	Rollback(ctx context.Context, id, rolloutID string, seq int64, target, baseline string) error
}

// Clock is injectable time.
type Clock func() time.Time

// Config controls gate timing and the health threshold.
type Config struct {
	// BatchWait bounds how long the gate waits for reports before deciding.
	BatchWait time.Duration
	// PollInterval is the decision-loop tick.
	PollInterval time.Duration
	// SuccessRatio is the default success ratio gate (0..1].
	SuccessRatio float64
	Logger       *log.Logger
}

type CreateRequest struct {
	ID            string   `json:"id"`
	TargetVersion string   `json:"target_version"`
	DeviceIDs     []string `json:"device_ids"`
	BatchSizes    []int    `json:"batch_sizes"`
	SuccessRatio  *float64 `json:"success_ratio,omitempty"`
}

// ReportAck explains how an incoming report was interpreted.
type ReportAck struct {
	Accepted bool   `json:"accepted"`
	State    string `json:"state,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

type orchestrator struct {
	st  store.Store
	cmd Commander
	cfg Config
	clk Clock
	log *log.Logger

	loopMu sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// seq source for commands so duplicate commands are detectable.
	seqMu sync.Mutex
	seq   int64
}

// New constructs an orchestrator.
func New(st store.Store, cmd Commander, cfg Config, clk Clock) Orchestrator {
	if clk == nil {
		clk = time.Now
	}
	if cfg.SuccessRatio <= 0 {
		cfg.SuccessRatio = 0.8
	}
	if cfg.BatchWait <= 0 {
		cfg.BatchWait = 500 * time.Millisecond
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 20 * time.Millisecond
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	return &orchestrator{st: st, cmd: cmd, cfg: cfg, clk: clk, log: cfg.Logger}
}

// Start hydrates the device fleet from persisted state (so a process restart
// can immediately dispatch to devices that already exist) and launches the
// background gate-evaluation loop.
func (o *orchestrator) Start(ctx context.Context) error {
	o.loopMu.Lock()
	defer o.loopMu.Unlock()
	if o.cancel != nil {
		return nil
	}
	if dm, ok := o.cmd.(deviceManager); ok {
		if err := o.st.View(ctx, func(v store.Viewer) error {
			for _, d := range v.Devices() {
				dm.AddDevice(d.ID, d.Version)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("hydrate devices: %w", err)
		}
	}
	inner, cancel := context.WithCancel(ctx)
	o.cancel = cancel
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		t := time.NewTicker(o.cfg.PollInterval)
		defer t.Stop()
		for {
			select {
			case <-inner.Done():
				return
			case <-t.C:
				_ = o.Tick(inner)
			}
		}
	}()
	return nil
}

// Shutdown stops the background loop.
func (o *orchestrator) Shutdown(ctx context.Context) error {
	o.loopMu.Lock()
	cancel := o.cancel
	o.cancel = nil
	o.loopMu.Unlock()
	if cancel != nil {
		cancel()
		done := make(chan struct{})
		go func() { o.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return o.st.Close()
}

// ---------- device registry ----------

func (o *orchestrator) UpsertDevice(ctx context.Context, id, version string) (*domain.Device, error) {
	if id == "" || version == "" {
		return nil, domain.ErrInvalidArgument
	}
	var saved *domain.Device
	err := o.st.Lock(ctx, func(mut store.Mutator) error {
		d := &domain.Device{ID: id, Version: version, Online: true, UpdatedAt: o.clk()}
		if existing, ok := mut.Device(id); ok {
			d.Online = existing.Online
			// Version is only writable here before the device ever joins a
			// rollout; afterwards firmware changes flow through reports.
			d.Version = existing.Version
		}
		if err := mut.UpsertDevice(d); err != nil {
			return err
		}
		saved = d
		return nil
	})
	if err == nil {
		if dm, ok := o.cmd.(deviceManager); ok {
			dm.AddDevice(id, saved.Version)
		}
	}
	return saved, err
}

func (o *orchestrator) RemoveDevice(ctx context.Context, id string) error {
	err := o.st.Lock(ctx, func(mut store.Mutator) error {
		if owner, ok := mut.DeviceLock(id); ok {
			return fmt.Errorf("%w: device locked by rollout %s", domain.ErrConflict, owner)
		}
		return mut.DeleteDevice(id)
	})
	if err == nil {
		if dm, ok := o.cmd.(deviceManager); ok {
			dm.RemoveDevice(id)
		}
	}
	return err
}

func (o *orchestrator) Devices(ctx context.Context) ([]*domain.Device, error) {
	var out []*domain.Device
	err := o.st.View(ctx, func(v store.Viewer) error {
		out = v.Devices()
		sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
		return nil
	})
	return out, err
}

// ---------- read models ----------

func (o *orchestrator) Rollout(ctx context.Context, id string) (*domain.Rollout, error) {
	var out *domain.Rollout
	err := o.st.View(ctx, func(v store.Viewer) error {
		r, ok := v.Rollout(id)
		if !ok {
			return domain.ErrNotFound
		}
		cp := *r
		out = &cp
		return nil
	})
	return out, err
}

func (o *orchestrator) Rollouts(ctx context.Context) ([]*domain.Rollout, error) {
	var out []*domain.Rollout
	err := o.st.View(ctx, func(v store.Viewer) error {
		out = v.Rollouts()
		sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
		return nil
	})
	return out, err
}

func (o *orchestrator) Participation(ctx context.Context, rolloutID, deviceID string) (*domain.Participation, error) {
	var out *domain.Participation
	err := o.st.View(ctx, func(v store.Viewer) error {
		p, ok := v.Participation(rolloutID, deviceID)
		if !ok {
			return domain.ErrNotFound
		}
		cp := *p
		out = &cp
		return nil
	})
	return out, err
}

func (o *orchestrator) Participants(ctx context.Context, rolloutID string) ([]*domain.Participation, error) {
	var out []*domain.Participation
	err := o.st.View(ctx, func(v store.Viewer) error {
		out = v.Participants(rolloutID)
		sort.Slice(out, func(i, j int) bool {
			if out[i].BatchIndex != out[j].BatchIndex {
				return out[i].BatchIndex < out[j].BatchIndex
			}
			return out[i].DeviceID < out[j].DeviceID
		})
		return nil
	})
	return out, err
}

func (o *orchestrator) Events(ctx context.Context, rolloutID string) ([]*domain.Event, error) {
	var out []*domain.Event
	err := o.st.View(ctx, func(v store.Viewer) error {
		out = v.Events(rolloutID)
		return nil
	})
	return out, err
}

// ---------- helpers ----------

func (o *orchestrator) nextCmdSeq() int64 {
	o.seqMu.Lock()
	defer o.seqMu.Unlock()
	o.seq++
	return o.seq
}

// emit appends a structured transition event inside a mutation.
func (o *orchestrator) emit(mut store.Mutator, rolloutID, deviceID, typ string, detail map[string]any) {
	mut.AppendEvent(&domain.Event{
		Time:      o.clk(),
		RolloutID: rolloutID,
		DeviceID:  deviceID,
		Type:      typ,
		Detail:    detail,
	})
}

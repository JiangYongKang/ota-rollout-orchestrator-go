package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/fleet"
)

// TestRestartStabilityAfterCompletion: completed conclusions survive a
// process restart; versions, phases and the event trail are unchanged.
func TestRestartStabilityAfterCompletion(t *testing.T) {
	env := newTestEnv(t, 80*time.Millisecond)
	ctx := context.Background()
	ids := env.seedDevices(3, "v1.0")
	if _, err := env.o.CreateRollout(ctx, CreateRequest{
		ID: tRolloutID, TargetVersion: "v2.0", DeviceIDs: ids, BatchSizes: []int{3},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	env.runUntil(t, 2*time.Second, func(r *domain.Rollout) bool {
		return r.Phase == domain.PhaseCompleted
	})
	eventsBefore, _ := env.o.Events(ctx, tRolloutID)
	env = env.reopen(t)

	// A few ticks after restart must not alter the settled conclusion.
	for i := 0; i < 5; i++ {
		if err := env.o.Tick(ctx); err != nil {
			t.Fatalf("tick after restart: %v", err)
		}
		env.clk.advance(time.Second)
	}
	r, _ := env.o.Rollout(ctx, tRolloutID)
	if r.Phase != domain.PhaseCompleted {
		t.Fatalf("phase after restart=%s", r.Phase)
	}
	for _, id := range ids {
		if ver, ok := env.fl.DeviceVersion(id); !ok || ver != "v2.0" {
			t.Fatalf("%s version=%s after restart", id, ver)
		}
		p, _ := env.o.Participation(ctx, tRolloutID, id)
		if p.State != domain.StateSuccess {
			t.Fatalf("%s state=%s after restart", id, p.State)
		}
	}
	eventsAfter, _ := env.o.Events(ctx, tRolloutID)
	if len(eventsAfter) != len(eventsBefore) {
		t.Fatalf("events changed after restart: before=%d after=%d",
			len(eventsBefore), len(eventsAfter))
	}
	// Devices are unlocked, so a new rollout can be created.
	if _, err := env.o.CreateRollout(ctx, CreateRequest{
		ID: "rl-2", TargetVersion: "v3.0", DeviceIDs: ids, BatchSizes: []int{3},
	}); err != nil {
		t.Fatalf("post-restart create: %v", err)
	}
	env.runUntilID(t, "rl-2", 2*time.Second, func(r *domain.Rollout) bool {
		return r.Phase == domain.PhaseCompleted
	})
}

// TestRestartDuringFailingReconciles: kill the process right after the gate
// fails; restart re-drives rollback purely from persisted state.
func TestRestartDuringFailingReconciles(t *testing.T) {
	env := newTestEnv(t, 400*time.Millisecond)
	ctx := context.Background()
	ids := env.seedDevices(3, "v1.0")
	env.fl.SetFault("dev-0", fleet.FaultInstallFail)
	if _, err := env.o.CreateRollout(ctx, CreateRequest{
		ID: tRolloutID, TargetVersion: "v2.0", DeviceIDs: ids, BatchSizes: []int{3},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	env.runUntil(t, 2*time.Second, func(r *domain.Rollout) bool {
		return r.Phase == domain.PhaseFailing
	})
	env = env.reopen(t)
	r := env.runUntil(t, 3*time.Second, func(r *domain.Rollout) bool {
		return r.Phase == domain.PhaseFailed
	})
	env.printStates(t, "after-restart-failed")
	for _, id := range ids {
		ver, _ := env.fl.DeviceVersion(id)
		if ver != "v1.0" {
			t.Fatalf("%s version=%s want v1.0 after restart-driven rollback", id, ver)
		}
	}
	if r.Batches[0].State != domain.BatchFailed {
		t.Fatalf("batch state=%s", r.Batches[0].State)
	}
}

func (e *testEnv) runUntilID(t *testing.T, id string, timeout time.Duration,
	cond func(r *domain.Rollout) bool) *domain.Rollout {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	var r *domain.Rollout
	for time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
		e.clk.advance(50 * time.Millisecond)
		_ = e.o.Tick(ctx)
		r, _ = e.o.Rollout(ctx, id)
		if cond(r) {
			return r
		}
	}
	t.Fatalf("condition not met for %s; phase=%s", id, r.Phase)
	return nil
}

package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/fleet"
)

// TestOfflineReconnectQueuedReport: the device is set offline immediately;
// install commands are undeliverable so it is classified OFFLINE at the
// deadline (missing report never counts as success). When it reconnects
// before the rollout settles, install + rollback commands flow and the
// device converges deterministically.
func TestOfflineReconnectQueuedReport(t *testing.T) {
	env := newTestEnv(t, 300*time.Millisecond)
	ctx := context.Background()
	ids := env.seedDevices(3, "v1.0") // sorted: dev-0,dev-1,dev-2
	env.fl.SetOnline("dev-2", false)

	if _, err := env.o.CreateRollout(ctx, CreateRequest{
		ID: tRolloutID, TargetVersion: "v2.0", DeviceIDs: ids, BatchSizes: []int{3},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Wait until the gate decision marks dev-2 OFFLINE (missing report never
	// counted as success) but the rollback window is still open.
	env.runUntil(t, 2*time.Second, func(r *domain.Rollout) bool {
		p, _ := env.o.Participation(ctx, tRolloutID, "dev-2")
		return p.State == domain.StateOffline
	})
	r, _ := env.o.Rollout(ctx, tRolloutID)
	if r.Phase != domain.PhaseFailing {
		t.Fatalf("phase=%s want FAILING (rollback in progress)", r.Phase)
	}
	if r.Batches[0].Offline < 1 {
		t.Fatalf("offline count=%d, want >=1", r.Batches[0].Offline)
	}

	// Reconnect while reconciliation is still retrying within its window:
	// the queued path converges install/rollback deterministically. Wait for
	// the rollout-level terminal phase (the state that is set by the tick that
	// observes the settled rollback), not just the per-device report.
	env.fl.SetOnline("dev-2", true)
	r = env.runUntil(t, 2*time.Second, func(r *domain.Rollout) bool {
		return r.Phase == domain.PhaseFailed
	})
	env.printStates(t, "after-reconnect")

	p, _ := env.o.Participation(ctx, tRolloutID, "dev-2")
	if p.State != domain.StateRolledBack {
		t.Fatalf("dev-2 state=%s want ROLLED_BACK", p.State)
	}
	if ver, _ := env.fl.DeviceVersion("dev-2"); ver != "v1.0" {
		t.Fatalf("dev-2 version=%s want baseline v1.0", ver)
	}
	if r.Phase != domain.PhaseFailed {
		t.Fatalf("phase=%s want FAILED (reconnect cannot resurrect a failed gate)", r.Phase)
	}
}

// TestOfflineReconnectAfterHealthyGate: a late device's delayed success cannot
// rewrite the authoritative completion; its version remains the baseline.
func TestOfflineLateReportCannotRewriteCompletion(t *testing.T) {
	env := newTestEnv(t, 300*time.Millisecond)
	ctx := context.Background()
	ids := env.seedDevices(2, "v1.0")
	// dev-1 receives commands but never reports during the first window.
	env.fl.SetFault("dev-1", fleet.FaultNoOp)

	if _, err := env.o.CreateRollout(ctx, CreateRequest{
		ID: tRolloutID, TargetVersion: "v2.0", DeviceIDs: ids, BatchSizes: []int{2},
		SuccessRatio: floatPtr(0.5),
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	r := env.runUntil(t, 2*time.Second, func(r *domain.Rollout) bool {
		return r.Phase == domain.PhaseCompleted
	})
	if r.Phase != domain.PhaseCompleted {
		t.Fatalf("phase=%s", r.Phase)
	}

	// A duplicated/late success arrives after completion: must be refused.
	ack, err := env.o.HandleReport(ctx, domain.Report{
		DeviceID: "dev-1", RolloutID: tRolloutID, Seq: 2,
		CurrentVersion: "v2.0", InstallOK: true,
	})
	if err != nil {
		t.Fatalf("report: %v", err)
	}
	if ack.Accepted {
		t.Fatalf("late report accepted: %+v", ack)
	}
	if ver, _ := env.fl.DeviceVersion("dev-1"); ver != "v1.0" {
		t.Fatalf("dev-1 version changed to %s after completion", ver)
	}
	t.Logf("late report correctly rejected: %s", ack.Reason)
}

func floatPtr(f float64) *float64 { return &f }

package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/fleet"
)

// TestUnhealthyBatchRollsBackAndStops: one failed install + one missing
// report in a 5-device batch, ratio 0.8 -> required 4 -> gate fails; the
// whole batch is rolled back to baseline and later batches never start.
func TestUnhealthyBatchRollsBackAndStops(t *testing.T) {
	env := newTestEnv(t, 80*time.Millisecond)
	ctx := context.Background()
	ids := env.seedDevices(7, "v1.0")

	// dev-2 install fails; dev-4 never receives its command (missing report).
	env.fl.SetFault("dev-2", fleet.FaultInstallFail)
	env.fl.SetFault("dev-4", fleet.FaultDropCommand)

	if _, err := env.o.CreateRollout(ctx, CreateRequest{
		ID: tRolloutID, TargetVersion: "v2.0", DeviceIDs: ids, BatchSizes: []int{5, 2},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	r := env.runUntil(t, 3*time.Second, func(r *domain.Rollout) bool {
		return r.Phase == domain.PhaseFailed
	})
	env.printStates(t, "failed-gate")

	if r.Batches[0].State != domain.BatchFailed {
		t.Fatalf("batch0=%s want FAILED", r.Batches[0].State)
	}
	if r.Batches[1].State != domain.BatchPending {
		t.Fatalf("batch1=%s want PENDING (never started)", r.Batches[1].State)
	}
	if got := r.Batches[0].Required; got != 4 {
		t.Fatalf("required=%d want 4 (ceil(5*0.8))", got)
	}

	ps, _ := env.o.Participants(ctx, tRolloutID)
	states := map[string]domain.DeviceState{}
	versions := map[string]string{}
	for _, p := range ps {
		states[p.DeviceID] = p.State
		versions[p.DeviceID], _ = env.fl.DeviceVersion(p.DeviceID)
	}
	// dev-4 never received any command (DROP_COMMAND incl. rollback): it cannot
	// confirm rollback within the window and is deterministically queued as
	// ROLLBACK_FAILED for manual handling, while staying on its baseline image.
	if states["dev-4"] != domain.StateRollbackFailed {
		t.Fatalf("dev-4 state=%s want ROLLBACK_FAILED (manual backlog)", states["dev-4"])
	}
	if versions["dev-4"] != "v1.0" {
		t.Fatalf("dev-4 version=%s want baseline v1.0", versions["dev-4"])
	}
	for _, id := range ids[:5] {
		if id == "dev-4" {
			continue
		}
		if states[id] != domain.StateRolledBack {
			t.Fatalf("%s state=%s want ROLLED_BACK", id, states[id])
		}
		if versions[id] != "v1.0" {
			t.Fatalf("%s version=%s want baseline v1.0", id, versions[id])
		}
	}
	// Untouched second batch never left pending and kept baseline version.
	for _, id := range ids[5:] {
		if states[id] != domain.StatePending {
			t.Fatalf("%s state=%s want PENDING", id, states[id])
		}
		if versions[id] != "v1.0" {
			t.Fatalf("%s version=%s want v1.0", id, versions[id])
		}
	}
}

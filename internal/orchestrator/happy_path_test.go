package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/domain"
)

// TestHappyPathBatchedPromotion verifies two healthy batches promote and the
// rollout completes with every device on the target version.
func TestHappyPathBatchedPromotion(t *testing.T) {
	env := newTestEnv(t, 80*time.Millisecond)
	ctx := context.Background()
	ids := env.seedDevices(5, "v1.0")

	if _, err := env.o.CreateRollout(ctx, CreateRequest{
		ID: tRolloutID, TargetVersion: "v2.0", DeviceIDs: ids, BatchSizes: []int{2, 3},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	r := env.runUntil(t, 2*time.Second, func(r *domain.Rollout) bool {
		return r.Phase == domain.PhaseCompleted
	})
	env.printStates(t, "completed")

	if r.Phase != domain.PhaseCompleted {
		t.Fatalf("phase=%s", r.Phase)
	}
	for i, b := range r.Batches {
		if b.State != domain.BatchCompleted {
			t.Fatalf("batch %d state=%s", i, b.State)
		}
		if b.Success != b.Size {
			t.Fatalf("batch %d success=%d size=%d", i, b.Success, b.Size)
		}
	}
	ps, _ := env.o.Participants(ctx, tRolloutID)
	for _, p := range ps {
		if p.State != domain.StateSuccess {
			t.Fatalf("%s state=%s want SUCCESS", p.DeviceID, p.State)
		}
		ver, ok := env.fl.DeviceVersion(p.DeviceID)
		if !ok || ver != "v2.0" {
			t.Fatalf("%s version=%s want v2.0", p.DeviceID, ver)
		}
	}
	// Devices unlocked after completion.
	devs, _ := env.o.Devices(ctx)
	for _, d := range devs {
		if err := env.o.RemoveDevice(ctx, d.ID); err != nil {
			t.Fatalf("device %s still locked: %v", d.ID, err)
		}
	}
}

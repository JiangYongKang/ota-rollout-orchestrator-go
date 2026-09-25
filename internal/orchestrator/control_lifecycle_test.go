package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/domain"
)

// TestPauseResume: pause immediately after creation; ticking many times must
// not start any batch or dispatch any install. Resume then completes normally.
func TestPauseResume(t *testing.T) {
	env := newTestEnv(t, 80*time.Millisecond)
	ctx := context.Background()
	ids := env.seedDevices(3, "v1.0")
	if _, err := env.o.CreateRollout(ctx, CreateRequest{
		ID: tRolloutID, TargetVersion: "v2.0", DeviceIDs: ids, BatchSizes: []int{3},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := env.o.PauseRollout(ctx, tRolloutID); err != nil {
		t.Fatalf("pause: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := env.o.Tick(ctx); err != nil {
			t.Fatalf("tick: %v", err)
		}
		env.clk.advance(time.Second)
		time.Sleep(3 * time.Millisecond)
	}
	r, _ := env.o.Rollout(ctx, tRolloutID)
	if r.Phase != domain.PhasePaused || r.Batches[0].State != domain.BatchPending {
		t.Fatalf("paused rollout progressed: phase=%s batch=%s", r.Phase, r.Batches[0].State)
	}
	t.Logf("pause held: phase=%s batch=%s (no dispatch)", r.Phase, r.Batches[0].State)

	if _, err := env.o.ResumeRollout(ctx, tRolloutID); err != nil {
		t.Fatalf("resume: %v", err)
	}
	r = env.runUntil(t, 2*time.Second, func(r *domain.Rollout) bool {
		return r.Phase == domain.PhaseCompleted
	})
	if r.Batches[0].Success != 3 {
		t.Fatalf("success=%d want 3", r.Batches[0].Success)
	}
}

// TestTerminateRollsEveryoneBack: terminate while batch 0 is healthy and batch
// 1 never started; devices on the target image return to baseline, the second
// batch is SKIPPED.
func TestTerminateRollsEveryoneBack(t *testing.T) {
	env := newTestEnv(t, 80*time.Millisecond)
	ctx := context.Background()
	ids := env.seedDevices(4, "v1.0")
	if _, err := env.o.CreateRollout(ctx, CreateRequest{
		ID: tRolloutID, TargetVersion: "v2.0", DeviceIDs: ids, BatchSizes: []int{2, 2},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Let batch 0 become healthy and batch 1 start, then terminate.
	env.runUntil(t, 2*time.Second, func(r *domain.Rollout) bool {
		return r.Cursor >= 1 && r.Batches[1].State == domain.BatchInProgress
	})
	if _, err := env.o.TerminateRollout(ctx, tRolloutID); err != nil {
		t.Fatalf("terminate: %v", err)
	}
	r := env.runUntil(t, 2*time.Second, func(r *domain.Rollout) bool {
		return r.Phase == domain.PhaseTerminated
	})
	env.printStates(t, "terminated")

	if r.Batches[0].State != domain.BatchHealthy && r.Batches[0].State != domain.BatchCompleted {
		t.Fatalf("batch0=%s", r.Batches[0].State)
	}
	// Batch 1 had already started when terminate arrived: no gate decision was
	// ever reached, so it is recorded FAILED(ABORTED); had terminate arrived
	// first it would instead be SKIPPED.
	if r.Batches[1].State != domain.BatchFailed {
		t.Fatalf("batch1=%s want FAILED (aborted mid-batch)", r.Batches[1].State)
	}
	ps, _ := env.o.Participants(ctx, tRolloutID)
	for _, p := range ps {
		if p.State != domain.StateRolledBack {
			t.Fatalf("%s state=%s want ROLLED_BACK", p.DeviceID, p.State)
		}
		if ver, _ := env.fl.DeviceVersion(p.DeviceID); ver != "v1.0" {
			t.Fatalf("%s version=%s want baseline", p.DeviceID, ver)
		}
	}
}

// TestTerminateIsIdempotent: repeated terminate never errors after settling.
func TestControlConflicts(t *testing.T) {
	env := newTestEnv(t, 80*time.Millisecond)
	ctx := context.Background()
	ids := env.seedDevices(2, "v1.0")
	if _, err := env.o.CreateRollout(ctx, CreateRequest{
		ID: tRolloutID, TargetVersion: "v2.0", DeviceIDs: ids, BatchSizes: []int{2},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := env.o.ResumeRollout(ctx, tRolloutID); err == nil {
		t.Fatal("resume of non-paused rollout must conflict")
	}
	env.runUntil(t, 2*time.Second, func(r *domain.Rollout) bool {
		return r.Phase == domain.PhaseCompleted
	})
	if _, err := env.o.PauseRollout(ctx, tRolloutID); err == nil {
		t.Fatal("pause of completed rollout must conflict")
	}
	if _, err := env.o.TerminateRollout(ctx, tRolloutID); err == nil {
		t.Fatal("terminate of completed rollout must conflict")
	}
}

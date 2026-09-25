package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/domain"
)

// TestConcurrentTicksAndBurstReports:
//  1. block all device reports at the sink while commands dispatch;
//  2. run many concurrent Tick() calls (simulating several entry points
//     evaluating the same rollout) - gate must decide at most once;
//  3. release the reports as one racing burst (order is non-deterministic);
//  4. keep hammering Tick until completion.
//
// Every device converges exactly once to SUCCESS on v2.0.
func TestConcurrentTicksAndBurstReports(t *testing.T) {
	env := newTestEnv(t, 200*time.Millisecond)
	ctx := context.Background()
	const n = 8
	ids := env.seedDevices(n, "v1.0")
	if _, err := env.o.CreateRollout(ctx, CreateRequest{
		ID: tRolloutID, TargetVersion: "v2.0",
		DeviceIDs: ids, BatchSizes: []int{n},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	env.gate.close()

	// First tick starts the rollout (PENDING -> RUNNING); the second tick
	// dispatches every install command, whose reports then pile at the gate.
	if err := env.o.Tick(ctx); err != nil {
		t.Fatalf("start tick: %v", err)
	}
	if err := env.o.Tick(ctx); err != nil {
		t.Fatalf("dispatch tick: %v", err)
	}
	p0, _ := env.o.Participation(ctx, tRolloutID, ids[0])
	if p0.State != domain.StateInstalling {
		t.Fatalf("state=%s want INSTALLING after dispatch", p0.State)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = env.o.Tick(ctx)
				}
			}
		}()
	}

	// Before the deadline, gated reports + concurrent ticks change nothing.
	env.clk.advance(50 * time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	r, _ := env.o.Rollout(ctx, tRolloutID)
	if r.Batches[0].State != domain.BatchInProgress {
		t.Fatalf("batch=%s want IN_PROGRESS while reports blocked", r.Batches[0].State)
	}

	// Burst: all devices' reports now race into HandleReport concurrently.
	env.gate.release()

	// Wait (still BEFORE the logical deadline) until every racing report has
	// been applied: gate timing must not beat in-flight reports in this test.
	reportDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(reportDeadline) {
		succ := 0
		for _, id := range ids {
			if pp, _ := env.o.Participation(ctx, tRolloutID, id); pp.State == domain.StateSuccess {
				succ++
			}
		}
		if succ == n {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	for _, id := range ids {
		pp, _ := env.o.Participation(ctx, tRolloutID, id)
		if pp.State != domain.StateSuccess {
			t.Fatalf("%s state=%s want SUCCESS after burst", id, pp.State)
		}
	}

	// Now let the logical deadline elapse; racing ticks converge once.
	env.clk.advance(300 * time.Millisecond)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r, _ = env.o.Rollout(ctx, tRolloutID)
		if r.Phase == domain.PhaseCompleted {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if r.Phase != domain.PhaseCompleted {
		t.Fatalf("phase=%s want COMPLETED under concurrent burst", r.Phase)
	}

	env.printStates(t, "concurrent-burst")
	ps, _ := env.o.Participants(ctx, tRolloutID)
	if len(ps) != n {
		t.Fatalf("participants=%d want %d (lost state)", len(ps), n)
	}
	events, _ := env.o.Events(ctx, tRolloutID)
	batchStarted := countEvents(events, "BATCH_STARTED")
	if batchStarted != 1 {
		t.Fatalf("BATCH_STARTED events=%d want exactly 1 (serialized gate)", batchStarted)
	}
	for _, p := range ps {
		if p.State != domain.StateSuccess {
			t.Fatalf("%s=%s", p.DeviceID, p.State)
		}
		if ver, _ := env.fl.DeviceVersion(p.DeviceID); ver != "v2.0" {
			t.Fatalf("%s version=%s", p.DeviceID, ver)
		}
	}
}

// TestConcurrentDuplicateReports sends the same terminal success report from
// many goroutines concurrently; none may be accepted after conclusion.
func TestConcurrentDuplicateReports(t *testing.T) {
	env := newTestEnv(t, 50*time.Millisecond)
	ctx := context.Background()
	ids := env.seedDevices(1, "v1.0")
	if _, err := env.o.CreateRollout(ctx, CreateRequest{
		ID: tRolloutID, TargetVersion: "v2.0",
		DeviceIDs: ids, BatchSizes: []int{1}, SuccessRatio: floatPtr(1),
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	env.runUntil(t, 2*time.Second, func(r *domain.Rollout) bool {
		return r.Phase == domain.PhaseCompleted
	})
	p, _ := env.o.Participation(ctx, tRolloutID, "dev-0")

	var wg sync.WaitGroup
	var acceptMu sync.Mutex
	accepted := 0
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ack, err := env.o.HandleReport(ctx, domain.Report{
				DeviceID: "dev-0", RolloutID: tRolloutID, Seq: p.LastSeq,
				CurrentVersion: "v2.0", InstallOK: true,
			})
			if err != nil {
				t.Errorf("report: %v", err)
				return
			}
			if ack.Accepted {
				acceptMu.Lock()
				accepted++
				acceptMu.Unlock()
			}
		}()
	}
	wg.Wait()
	if accepted != 0 {
		t.Fatalf("accepted=%d duplicate terminal reports, want 0", accepted)
	}
}

func countEvents(evs []*domain.Event, typ string) int {
	n := 0
	for _, e := range evs {
		if e.Type == typ {
			n++
		}
	}
	return n
}

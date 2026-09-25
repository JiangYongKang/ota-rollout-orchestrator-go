package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/fleet"
)

// TestDroppedReportReplayConverges: a device installs successfully but its
// success report is lost (DROP_REPORT). The batch fails the gate and starts
// rollback; rollback orchestrator retries use the *rollback* command seq.
// This test specifically verifies the fleet-side duplicate replay: once the
// report-drop fault is cleared, a repeat of the original install command (same
// seq) replays the cached result rather than installing twice.
func TestFleetDuplicateCommandReplaysCachedResult(t *testing.T) {
	env := newTestEnv(t, 80*time.Millisecond)
	ctx := context.Background()
	ids := env.seedDevices(1, "v1.0")
	env.fl.SetFault("dev-0", fleet.FaultDropReport)
	if _, err := env.o.CreateRollout(ctx, CreateRequest{
		ID: tRolloutID, TargetVersion: "v2.0", DeviceIDs: ids,
		BatchSizes: []int{1}, SuccessRatio: floatPtr(1),
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Dispatch; report is dropped -> device is on v2.0 but orchestrator sees
	// nothing until the deadline.
	_ = env.o.Tick(ctx)
	_ = env.o.Tick(ctx)
	waitFor := func(cond func() bool) {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
		t.Fatal("condition not met in time")
	}
	waitFor(func() bool {
		ver, _ := env.fl.DeviceVersion("dev-0")
		return ver == "v2.0"
	})

	// Clear the transport fault and re-send the SAME command (same seq): the
	// fleet replays the cached success report; it must not flash a second time.
	env.fl.SetFault("dev-0", fleet.FaultNone)
	p, _ := env.o.Participation(ctx, tRolloutID, "dev-0")
	if err := env.fl.Install(ctx, "dev-0", tRolloutID, p.LastSeq, "v1.0", "v2.0"); err != nil {
		t.Fatalf("replay install: %v", err)
	}
	waitFor(func() bool {
		pp, _ := env.o.Participation(ctx, tRolloutID, "dev-0")
		p = pp
		return p.State == domain.StateSuccess
	})
	if p.State != domain.StateSuccess {
		t.Fatalf("state=%s want SUCCESS after duplicate command replay", p.State)
	}
	if ver, _ := env.fl.DeviceVersion("dev-0"); ver != "v2.0" {
		t.Fatalf("version=%s", ver)
	}
	t.Logf("duplicate command seq=%d replayed cached success; state=%s reason=%q",
		p.LastSeq, p.State, p.Reason)
}

// TestUnknownDeviceInRolloutRejected guards report admission.
func TestReportFromTargetedButWrongVersion(t *testing.T) {
	env := newTestEnv(t, 80*time.Millisecond)
	ctx := context.Background()
	ids := env.seedDevices(1, "v1.0")
	if _, err := env.o.CreateRollout(ctx, CreateRequest{
		ID: tRolloutID, TargetVersion: "v2.0", DeviceIDs: ids, BatchSizes: []int{1},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	// A report that claims neither target nor baseline is ignored (no crash).
	ack, err := env.o.HandleReport(ctx, domain.Report{
		DeviceID: "dev-0", RolloutID: tRolloutID, Seq: 99,
		CurrentVersion: "v7.7", InstallOK: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ack.Accepted {
		t.Fatalf("unrelated version report accepted: %+v", ack)
	}
	p, _ := env.o.Participation(ctx, tRolloutID, "dev-0")
	if p.LastSeq != 0 {
		t.Fatalf("lastSeq mutated to %d by non-applicable report", p.LastSeq)
	}
}

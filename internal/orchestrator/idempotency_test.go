package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/domain"
)

// TestDuplicateAndOutOfOrderReports: wait for one device to succeed, then feed
// duplicate, out-of-order and contradictory reports by hand. Nothing may
// change the terminal conclusion or the confirmed version.
func TestDuplicateAndOutOfOrderReports(t *testing.T) {
	env := newTestEnv(t, 80*time.Millisecond)
	ctx := context.Background()
	ids := env.seedDevices(2, "v1.0")
	if _, err := env.o.CreateRollout(ctx, CreateRequest{
		ID: tRolloutID, TargetVersion: "v2.0", DeviceIDs: ids, BatchSizes: []int{2},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	env.runUntil(t, 2*time.Second, func(r *domain.Rollout) bool {
		return r.Phase == domain.PhaseCompleted
	})

	p0, _ := env.o.Participation(ctx, tRolloutID, "dev-0")
	seq := p0.LastSeq
	cases := []struct {
		name   string
		report domain.Report
		accept bool
	}{
		{"exact duplicate success", domain.Report{
			DeviceID: "dev-0", RolloutID: tRolloutID, Seq: seq,
			CurrentVersion: "v2.0", InstallOK: true}, false},
		{"stale older seq", domain.Report{
			DeviceID: "dev-0", RolloutID: tRolloutID, Seq: seq - 1,
			CurrentVersion: "v2.0", InstallOK: true}, false},
		{"contradictory failure after success", domain.Report{
			DeviceID: "dev-0", RolloutID: tRolloutID, Seq: seq + 100,
			CurrentVersion: "v1.0", InstallOK: false}, false},
		{"rollback to baseline invented by device", domain.Report{
			DeviceID: "dev-0", RolloutID: tRolloutID, Seq: seq + 101,
			CurrentVersion: "v1.0", InstallOK: true}, false},
	}
	for _, c := range cases {
		ack, err := env.o.HandleReport(ctx, c.report)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if ack.Accepted != c.accept {
			t.Fatalf("%s: accepted=%v want %v (%s)", c.name, ack.Accepted, c.accept, ack.Reason)
		}
		p, _ := env.o.Participation(ctx, tRolloutID, "dev-0")
		if p.State != domain.StateSuccess {
			t.Fatalf("%s: state mutated to %s", c.name, p.State)
		}
		if ver, _ := env.fl.DeviceVersion("dev-0"); ver != "v2.0" {
			t.Fatalf("%s: version mutated to %s", c.name, ver)
		}
		t.Logf("%s -> rejected: %s", c.name, ack.Reason)
	}
}

// TestDuplicateCreateDoesNotReinstall: creating the same rollout twice fails,
// and a second rollout targeting an already-locked device is rejected.
func TestDuplicateCreateConflict(t *testing.T) {
	env := newTestEnv(t, 80*time.Millisecond)
	ctx := context.Background()
	ids := env.seedDevices(2, "v1.0")
	req := CreateRequest{
		ID: tRolloutID, TargetVersion: "v2.0", DeviceIDs: ids, BatchSizes: []int{2},
	}
	if _, err := env.o.CreateRollout(ctx, req); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := env.o.CreateRollout(ctx, req); err == nil {
		t.Fatal("duplicate create must fail")
	}
	other := req
	other.ID = "rl-other"
	if _, err := env.o.CreateRollout(ctx, other); err == nil {
		t.Fatal("concurrent rollout on locked devices must fail")
	} else {
		t.Logf("conflict correctly reported: %v", err)
	}
}

// TestUnknownReportRejected ensures stray reports mutate nothing.
func TestUnknownReportRejected(t *testing.T) {
	env := newTestEnv(t, 80*time.Millisecond)
	ctx := context.Background()
	env.seedDevices(1, "v1.0")
	if _, err := env.o.HandleReport(ctx, domain.Report{
		DeviceID: "dev-0", RolloutID: "ghost", Seq: 1,
		CurrentVersion: "v9.9", InstallOK: true,
	}); err == nil {
		t.Fatal("report for unknown rollout must error")
	}
}

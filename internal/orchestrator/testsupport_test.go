package orchestrator

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/fleet"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/store"
)

type testEnv struct {
	path string
	st   store.Store
	fl   fleet.Fleet
	o    Orchestrator
	clk  *fakeClock
	gate *sinkGate
}

// sinkGate optionally blocks fleet reports until released, so tests can stage
// many in-flight reports and release them concurrently (reorder/duplicate).
type sinkGate struct {
	inner fleet.Sink
	mu    sync.Mutex
	open  bool
	cond  *sync.Cond
}

func newSinkGate(inner fleet.Sink) *sinkGate {
	g := &sinkGate{inner: inner, open: true}
	g.cond = sync.NewCond(&g.mu)
	return g
}

func (g *sinkGate) Deliver(ctx context.Context, r domain.Report) error {
	g.mu.Lock()
	for !g.open {
		g.cond.Wait()
	}
	g.mu.Unlock()
	return g.inner.Deliver(ctx, r)
}

func (g *sinkGate) release() {
	g.mu.Lock()
	g.open = true
	g.cond.Broadcast()
	g.mu.Unlock()
}

func (g *sinkGate) close() {
	g.mu.Lock()
	g.open = false
	g.mu.Unlock()
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
	return c.t
}

func newTestEnv(t *testing.T, wait time.Duration) *testEnv {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.json")
	env0 := &testEnv{path: path}
	st, err := store.New(path)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	fl := fleet.New(fleet.WithDelay(func() time.Duration { return 0 }))
	clk := &fakeClock{t: time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)}
	o := New(st, fl, Config{
		BatchWait:    wait,
		PollInterval: 5 * time.Millisecond,
		SuccessRatio: 0.8,
	}, clk.now)
	gate := newSinkGate(reportSink{o})
	fl.SetSink(gate)
	env0.gate = gate
	env0.st, env0.fl, env0.o, env0.clk = st, fl, o, clk
	return env0
}

// reopen closes and rebuilds store + orchestrator from disk, simulating a
// process restart. The fleet is rebuilt from the persisted device versions.
func (e *testEnv) reopen(t *testing.T) *testEnv {
	t.Helper()
	if err := e.st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	st, err := store.New(e.path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	fl := fleet.New(fleet.WithDelay(func() time.Duration { return 0 }))
	o := New(st, fl, Config{
		BatchWait:    e.o.(*orchestrator).cfg.BatchWait,
		PollInterval: 5 * time.Millisecond,
		SuccessRatio: e.o.(*orchestrator).cfg.SuccessRatio,
	}, e.clk.now)
	// Start() must hydrate the (empty) fleet purely from persisted state.
	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("start after reopen: %v", err)
	}
	t.Cleanup(func() {
		cx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = o.Shutdown(cx)
	})
	gate := newSinkGate(reportSink{o})
	fl.SetSink(gate)
	e.gate = gate
	e.st, e.fl, e.o = st, fl, o
	return e
}

// reportSink adapts the orchestrator to fleet.Sink.
type reportSink struct{ o Orchestrator }

func (s reportSink) Deliver(ctx context.Context, r domain.Report) error {
	_, err := s.o.HandleReport(ctx, r)
	return err
}

// runUntil drives ticks with real sleeps until cond is met or timeout elapses.
func (e *testEnv) runUntil(t *testing.T, timeout time.Duration,
	cond func(r *domain.Rollout) bool) *domain.Rollout {

	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	var r *domain.Rollout
	for time.Now().Before(deadline) {
		// Let simulated async reports (zero delay) land, then advance the
		// injected logical clock so batch/rollback deadlines elapse.
		time.Sleep(2 * time.Millisecond)
		e.clk.advance(50 * time.Millisecond)
		if err := e.o.Tick(ctx); err != nil {
			t.Fatalf("tick: %v", err)
		}
		var err error
		r, err = e.o.Rollout(ctx, tRolloutID)
		if err != nil {
			t.Fatalf("rollout: %v", err)
		}
		if cond(r) {
			return r
		}
	}
	t.Fatalf("condition not met before timeout; phase=%s", r.Phase)
	return nil
}

const tRolloutID = "rl-1"

func (e *testEnv) seedDevices(n int, version string) []string {
	ctx := context.Background()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id := "dev-" + itoa(i)
		if _, err := e.o.UpsertDevice(ctx, id, version); err != nil {
			panic(err)
		}
		ids = append(ids, id)
	}
	return ids
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}

func (e *testEnv) printStates(t *testing.T, label string) {
	t.Helper()
	ctx := context.Background()
	r, _ := e.o.Rollout(ctx, tRolloutID)
	t.Logf("--- %s: rollout=%s phase=%s reason=%q", label, r.ID, r.Phase, r.Reason)
	for i, b := range r.Batches {
		t.Logf("  batch[%d] state=%s success=%d failed=%d offline=%d required=%d reason=%q",
			i, b.State, b.Success, b.Failed, b.Offline, b.Required, b.Reason)
	}
	ps, _ := e.o.Participants(ctx, tRolloutID)
	for _, p := range ps {
		ver, _ := e.fl.DeviceVersion(p.DeviceID)
		t.Logf("  device=%s batch=%d state=%-15s ver=%-6s baseline=%-6s reason=%q",
			p.DeviceID, p.BatchIndex, p.State, ver, p.FromVersion, p.Reason)
	}
}

// Package fleet simulates a device fleet for local validation.
//
// Every simulated device:
//   - installs/rolls back asynchronously and reports the result through a Sink
//     (the orchestrator), so reports naturally race and may reorder;
//   - treats an exact duplicate command idempotently: if the work already
//     finished it replays the cached result (covers lost reports + retry), if
//     it is in-flight it does nothing extra, and offline it returns an error;
//   - supports deterministic fault injection (failed install, dropped command,
//     dropped report, no-op);
//   - queues its report while offline and flushes it on reconnect, so a
//     reconnecting device's conclusion still converges.
package fleet

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/domain"
)

// Sink receives reports emitted by simulated devices.
type Sink interface {
	Deliver(ctx context.Context, r domain.Report) error
}

// Fleet is the simulated device fleet.
type Fleet interface {
	AddDevice(id, version string)
	RemoveDevice(id string)
	DeviceIDs() []string
	DeviceVersion(id string) (string, bool)
	IsOnline(id string) bool
	SetOnline(id string, online bool)
	SetFault(id string, fault Fault)
	Install(ctx context.Context, id, rolloutID string, seq int64, from, target string) error
	Rollback(ctx context.Context, id, rolloutID string, seq int64, target, baseline string) error
	SetSink(s Sink)
}

// Fault is the injected behavior for a device.
type Fault string

const (
	FaultNone        Fault = ""
	FaultInstallFail Fault = "INSTALL_FAIL" // install result is a failure report
	FaultDropCommand Fault = "DROP_COMMAND" // commands vanish in transit, no ack
	FaultDropReport  Fault = "DROP_REPORT"  // result reports vanish in transit
	FaultNoOp        Fault = "NO_OP"        // command acked, device never acts
)

// Option configures a fleet.
type Option func(*fleet)

// WithDelay installs a custom report-delay function (default small jitter so
// concurrent devices report out of order).
func WithDelay(f func() time.Duration) Option {
	return func(fl *fleet) { fl.delay = f }
}

type cmdKey struct {
	rolloutID string
	seq       int64
}

type agent struct {
	mu       sync.Mutex
	id       string
	version  string
	online   bool
	fault    Fault
	inflight *cmdKey                  // command currently executing
	finished map[cmdKey]domain.Report // cached results for duplicate replay
	pending  *domain.Report           // report produced while offline
}

type fleet struct {
	mu      sync.Mutex
	devices map[string]*agent
	sink    Sink
	delay   func() time.Duration
	rng     *rand.Rand
}

// New builds an empty fleet.
func New(opts ...Option) Fleet {
	fl := &fleet{
		devices: map[string]*agent{},
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	fl.delay = func() time.Duration {
		return time.Duration(fl.rng.Intn(3)) * time.Millisecond
	}
	for _, o := range opts {
		o(fl)
	}
	return fl
}

func (fl *fleet) SetSink(s Sink) {
	fl.mu.Lock()
	fl.sink = s
	fl.mu.Unlock()
}

func (fl *fleet) get(id string) (*agent, error) {
	fl.mu.Lock()
	a, ok := fl.devices[id]
	fl.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: device %s", domain.ErrNotFound, id)
	}
	return a, nil
}

func (fl *fleet) AddDevice(id, version string) {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	if _, ok := fl.devices[id]; ok {
		return
	}
	fl.devices[id] = &agent{
		id: id, version: version, online: true,
		finished: map[cmdKey]domain.Report{},
	}
}

func (fl *fleet) RemoveDevice(id string) {
	fl.mu.Lock()
	delete(fl.devices, id)
	fl.mu.Unlock()
}

func (fl *fleet) DeviceIDs() []string {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	out := make([]string, 0, len(fl.devices))
	for id := range fl.devices {
		out = append(out, id)
	}
	return out
}

func (fl *fleet) DeviceVersion(id string) (string, bool) {
	a, err := fl.get(id)
	if err != nil {
		return "", false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.version, true
}

func (fl *fleet) IsOnline(id string) bool {
	a, err := fl.get(id)
	if err != nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.online
}

func (fl *fleet) SetFault(id string, fault Fault) {
	a, err := fl.get(id)
	if err != nil {
		return
	}
	a.mu.Lock()
	a.fault = fault
	a.mu.Unlock()
}

func (fl *fleet) SetOnline(id string, online bool) {
	a, err := fl.get(id)
	if err != nil {
		return
	}
	var queued *domain.Report
	a.mu.Lock()
	wasOnline := a.online
	a.online = online
	if online && !wasOnline {
		queued = a.pending
		a.pending = nil
	}
	a.mu.Unlock()
	if queued != nil {
		fl.deliver(*queued)
	}
}

func (fl *fleet) Install(ctx context.Context, id, rolloutID string, seq int64, from, target string) error {
	return fl.dispatch(id, cmdKey{rolloutID, seq}, "install", from, target)
}

func (fl *fleet) Rollback(ctx context.Context, id, rolloutID string, seq int64, target, baseline string) error {
	// Report.FromVersion = current(target image), CurrentVersion = baseline.
	return fl.dispatch(id, cmdKey{rolloutID, seq}, "rollback", target, baseline)
}

// dispatch applies one command. For install: from=baseline, wanted=target;
// for rollback: from=target image, wanted=baseline.
func (fl *fleet) dispatch(id string, k cmdKey, kind, from, wanted string) error {
	a, err := fl.get(id)
	if err != nil {
		return err
	}

	a.mu.Lock()
	if !a.online {
		a.mu.Unlock()
		return fmt.Errorf("device %s offline: command undeliverable", id)
	}
	if cached, ok := a.finished[k]; ok {
		// Retransmission of a finished command: replay its result once.
		online := a.online
		a.mu.Unlock()
		if online {
			fl.deliver(cached)
		}
		return nil
	}
	if a.inflight != nil && *a.inflight == k {
		a.mu.Unlock()
		return nil // already executing this exact command
	}
	fault := a.fault
	if fault == FaultDropCommand {
		// Transport accepted the call but the device never received it.
		a.mu.Unlock()
		return nil
	}
	inflight := k
	a.inflight = &inflight
	a.mu.Unlock()

	go func() {
		time.Sleep(fl.delay())
		r := fl.execute(a, k, kind, from, wanted, fault)
		fl.afterExecute(a, k, r, fault)
	}()
	return nil
}

// execute mutates the simulated firmware and builds the result report.
func (fl *fleet) execute(a *agent, k cmdKey, kind, from, wanted string, fault Fault) domain.Report {
	r := domain.Report{DeviceID: a.id, RolloutID: k.rolloutID, Seq: k.seq, Online: true}
	a.mu.Lock()
	defer a.mu.Unlock()

	switch {
	case fault == FaultNoOp:
		// Acked but never acts: no result is ever produced.
		a.inflight = nil
		return domain.Report{} // empty sentinel
	case kind == "rollback":
		// Rolling back to the captured known-good image is reliable.
		r.InstallOK = true
		r.FromVersion = from
		r.CurrentVersion = wanted
		a.version = wanted
	case fault == FaultInstallFail:
		r.InstallOK = false
		r.FromVersion = from
		r.CurrentVersion = from // stayed put
	default:
		r.InstallOK = true
		r.FromVersion = from
		r.CurrentVersion = wanted
		a.version = wanted
	}
	a.inflight = nil
	return r
}

// afterExecute caches the result and then delivers it, honoring offline queue
// and report-drop faults.
func (fl *fleet) afterExecute(a *agent, k cmdKey, r domain.Report, fault Fault) {
	if (r == domain.Report{}) {
		return
	}
	a.mu.Lock()
	a.finished[k] = r
	if !a.online {
		a.pending = &r
		a.mu.Unlock()
		return
	}
	drop := fault == FaultDropReport
	a.mu.Unlock()
	if drop {
		return // report lost; the orchestrator retries with the same command seq
	}
	fl.deliver(r)
}

func (fl *fleet) deliver(r domain.Report) {
	fl.mu.Lock()
	s := fl.sink
	fl.mu.Unlock()
	if s == nil {
		return
	}
	_ = s.Deliver(context.Background(), r)
}

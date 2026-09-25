package orchestrator

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/store"
)

// pendingCommand is a device instruction collected inside a transaction and
// dispatched after the store lock is released.
type pendingCommand struct {
	deviceID string
	rollback bool
	seq      int64
	from     string
	target   string
}

// rollbackWindow bounds how long a rollback confirmation is waited for before
// the device is marked ROLLBACK_FAILED for manual handling.
const rollbackWindowFactor = 2

// Tick evaluates every non-terminal rollout exactly once. It is:
//   - idempotent: repeated ticks (also across restarts) only fill gaps;
//   - safe under concurrency: all reads/writes are one store transaction;
//   - non-blocking: device I/O happens after the lock via pendingCommands.
func (o *orchestrator) Tick(ctx context.Context) error {
	var ids []string
	if err := o.st.View(ctx, func(v store.Viewer) error {
		for _, r := range v.Rollouts() {
			switch r.Phase {
			case domain.PhasePending, domain.PhaseRunning, domain.PhaseFailing,
				domain.PhaseTerminating, domain.PhasePaused:
				ids = append(ids, r.ID)
			}
		}
		sort.Strings(ids)
		return nil
	}); err != nil {
		return err
	}
	for _, id := range ids {
		if err := o.tickRollout(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (o *orchestrator) tickRollout(ctx context.Context, id string) error {
	var cmds []pendingCommand
	err := o.st.Lock(ctx, func(mut store.Mutator) error {
		r, ok := mut.Rollout(id)
		if !ok {
			return nil
		}
		roll := *r

		// Rollback reconciliation runs in every non-terminal phase where it may
		// be pending (failed-gate isolation, terminate cascade, paused mid-failure).
		cmds = append(cmds, o.collectRollbacks(mut, &roll)...)

		switch roll.Phase {
		case domain.PhasePending:
			roll.Phase = domain.PhaseRunning
			roll.Reason = "started"
		case domain.PhasePaused:
			// Reports keep being applied; promotion/dispatch is frozen.
		case domain.PhaseRunning:
			cmds = append(cmds, o.advanceRunning(mut, &roll)...)
		case domain.PhaseFailing:
			o.evaluateFailing(mut, &roll)
		case domain.PhaseTerminating:
			o.evaluateTerminating(mut, &roll)
		}

		roll.UpdatedAt = o.clk()
		return mut.SaveRollout(&roll)
	})
	if err != nil {
		return err
	}
	o.dispatch(ctx, id, cmds)
	return nil
}

// advanceRunning dispatches the current batch and applies the gate decision.
func (o *orchestrator) advanceRunning(mut store.Mutator, roll *domain.Rollout) []pendingCommand {
	var cmds []pendingCommand
	b := &roll.Batches[roll.Cursor]
	now := o.clk()

	if b.State == domain.BatchPending {
		b.State = domain.BatchInProgress
		b.StartedAt = &now
		deadline := now.Add(o.cfg.BatchWait)
		b.Deadline = &deadline
		o.emit(mut, roll.ID, "", "BATCH_STARTED", map[string]any{
			"batch": b.Index, "size": b.Size, "deadline": deadline,
		})
		o.log.Printf("rollout=%s batch=%d STARTED size=%d deadline=%s",
			roll.ID, b.Index, b.Size, deadline.Format("15:04:05.000"))
	}

	if b.State == domain.BatchInProgress {
		for _, p := range batchParticipants(mut, roll.ID, b.Index) {
			if p.State != domain.StatePending {
				continue
			}
			seq := o.nextCmdSeq()
			cp := *p
			cp.State = domain.StateInstalling
			cp.LastSeq = seq
			cp.Reason = "install command dispatched"
			cp.UpdatedAt = now
			_ = mut.SaveParticipation(&cp)
			o.emit(mut, roll.ID, p.DeviceID, "DEVICE_INSTALL_DISPATCHED", map[string]any{
				"seq": seq, "target": roll.TargetVersion,
			})
			cmds = append(cmds, pendingCommand{
				deviceID: p.DeviceID, seq: seq,
				from: p.FromVersion, target: roll.TargetVersion,
			})
		}
	}

	o.evaluateBatch(mut, roll, b)
	return cmds
}

// evaluateBatch applies missing-report timeout and the health gate once the
// batch deadline has elapsed.
func (o *orchestrator) evaluateBatch(mut store.Mutator, roll *domain.Rollout, b *domain.Batch) {
	if b.State != domain.BatchInProgress || b.Deadline == nil || o.clk().Before(*b.Deadline) {
		return
	}
	now := o.clk()
	ps := batchParticipants(mut, roll.ID, b.Index)

	var success, failed, offline int
	for _, p := range ps {
		switch p.State {
		case domain.StateSuccess:
			success++
		case domain.StateFailed:
			failed++
		case domain.StateOffline:
			offline++
		case domain.StatePending, domain.StateInstalling:
			// Report never arrived within the window: a missing report is NOT a
			// success; classify OFFLINE so the gate and the reason are explicit.
			offline++
			cp := *p
			cp.State = domain.StateOffline
			cp.Reason = "no report before batch deadline; treated as offline/missing"
			cp.UpdatedAt = now
			_ = mut.SaveParticipation(&cp)
			o.emit(mut, roll.ID, p.DeviceID, "DEVICE_OFFLINE", map[string]any{
				"batch": b.Index, "reason": cp.Reason,
			})
		}
	}
	b.Success, b.Failed, b.Offline = success, failed, offline

	required := int(math.Ceil(float64(b.Size) * roll.SuccessRatio))
	b.Required = required
	decided := now

	if success >= required {
		b.State = domain.BatchHealthy
		b.Deadline = nil
		b.DecidedAt = &decided
		b.Reason = "health gate passed"
		o.emit(mut, roll.ID, "", "BATCH_HEALTHY", map[string]any{
			"batch": b.Index, "success": success, "failed": failed,
			"offline": offline, "required": required, "ratio": roll.SuccessRatio,
		})
		o.log.Printf("rollout=%s batch=%d HEALTHY success=%d failed=%d offline=%d required=%d -> gate PASSED",
			roll.ID, b.Index, success, failed, offline, required)

		if b.Index == len(roll.Batches)-1 {
			for i := range roll.Batches {
				if roll.Batches[i].State == domain.BatchHealthy {
					roll.Batches[i].State = domain.BatchCompleted
				}
			}
			roll.Phase = domain.PhaseCompleted
			roll.Reason = "all batches healthy; missing-report devices stay isolated as OFFLINE"
			o.emit(mut, roll.ID, "", "ROLLOUT_COMPLETED", map[string]any{
				"success": success, "offline": offline,
			})
			o.releaseLocks(mut, roll.ID)
		} else {
			roll.Cursor = b.Index + 1
		}
		return
	}

	// Unhealthy: gate failed deterministically at the deadline. Isolate this
	// batch and roll it back; earlier healthy batches are retained.
	b.State = domain.BatchFailing
	b.Deadline = nil
	b.DecidedAt = &decided
	b.Reason = "health gate failed"
	roll.Phase = domain.PhaseFailing
	roll.Reason = "batch failed health gate; isolating and rolling back the batch"
	o.emit(mut, roll.ID, "", "BATCH_UNHEALTHY", map[string]any{
		"batch": b.Index, "success": success, "failed": failed,
		"offline": offline, "required": required, "ratio": roll.SuccessRatio,
	})
	o.log.Printf("rollout=%s batch=%d UNHEALTHY success=%d failed=%d offline=%d required=%d -> ROLLBACK batch and STOP",
		roll.ID, b.Index, success, failed, offline, required)
}

// collectRollbacks drives every device back to its known-good baseline.
//
// Failing phase: every device in the failed batch (including FAILED/OFFLINE
// devices that never confirmed the target) is sent a rollback, so the result
// is uniformly "back on a known available image" or ROLLBACK_FAILED.
// Terminating phase: target-image devices are rolled back; devices that were
// never dispatched are directly ROLLED_BACK on their baseline.
// Retries reuse the original command seq; unconfirmed rollbacks past the
// rollback window become ROLLBACK_FAILED (manual backlog).
func (o *orchestrator) collectRollbacks(mut store.Mutator, roll *domain.Rollout) []pendingCommand {
	var cmds []pendingCommand
	now := o.clk()
	window := o.cfg.BatchWait * time.Duration(rollbackWindowFactor)

	issue := func(p *domain.Participation) {
		seq := o.nextCmdSeq()
		cp := *p
		cp.State = domain.StateRollingBack
		cp.LastSeq = seq
		since := now
		cp.RollbackSince = &since
		cp.Reason = "rollback to captured baseline dispatched"
		cp.UpdatedAt = now
		_ = mut.SaveParticipation(&cp)
		o.emit(mut, roll.ID, p.DeviceID, "DEVICE_ROLLBACK_DISPATCHED", map[string]any{
			"baseline": p.FromVersion, "seq": seq,
		})
		cmds = append(cmds, pendingCommand{
			deviceID: p.DeviceID, rollback: true, seq: seq,
			from: roll.TargetVersion, target: p.FromVersion,
		})
	}

	retryOrExpire := func(p *domain.Participation) {
		if p.RollbackSince != nil && now.Sub(*p.RollbackSince) > window {
			cp := *p
			cp.State = domain.StateRollbackFailed
			cp.Reason = "rollback unconfirmed past window; queued for manual handling"
			cp.UpdatedAt = now
			_ = mut.SaveParticipation(&cp)
			o.emit(mut, roll.ID, p.DeviceID, "DEVICE_ROLLBACK_FAILED", map[string]any{
				"reason": cp.Reason,
			})
			return
		}
		// Same command number: a device that got the command but whose report
		// was lost answers again (fleet idempotency); a still-offline device
		// simply fails delivery again and is retried on the next tick.
		cmds = append(cmds, pendingCommand{
			deviceID: p.DeviceID, rollback: true, seq: p.LastSeq,
			from: roll.TargetVersion, target: p.FromVersion,
		})
	}

	for _, p := range mut.Participants(roll.ID) {
		switch roll.Phase {
		case domain.PhaseFailing:
			if p.BatchIndex != roll.Cursor {
				continue
			}
			switch p.State {
			case domain.StateSuccess, domain.StateFailed, domain.StateOffline,
				domain.StatePending, domain.StateInstalling:
				issue(p)
			case domain.StateRollingBack:
				retryOrExpire(p)
			}
		case domain.PhaseTerminating:
			switch p.State {
			case domain.StateSuccess, domain.StateFailed, domain.StateOffline,
				domain.StateInstalling:
				issue(p)
			case domain.StatePending:
				// Never dispatched: remains on baseline by construction.
				cp := *p
				cp.State = domain.StateRolledBack
				cp.Reason = "never dispatched before termination; stays on known baseline"
				cp.UpdatedAt = now
				_ = mut.SaveParticipation(&cp)
				o.emit(mut, roll.ID, p.DeviceID, "DEVICE_ROLLED_BACK", map[string]any{
					"reason": cp.Reason, "version": p.FromVersion,
				})
			case domain.StateRollingBack:
				retryOrExpire(p)
			}
		case domain.PhasePaused:
			if p.State == domain.StateRollingBack {
				retryOrExpire(p)
			}
		}
	}
	return cmds
}

// evaluateFailing finalizes the failed batch once reconciliation settles.
func (o *orchestrator) evaluateFailing(mut store.Mutator, roll *domain.Rollout) {
	b := &roll.Batches[roll.Cursor]
	var rolledBack, rbFailed, unresolved int
	for _, p := range batchParticipants(mut, roll.ID, b.Index) {
		switch p.State {
		case domain.StateRolledBack:
			rolledBack++
		case domain.StateRollbackFailed:
			rbFailed++
		default:
			unresolved++
		}
	}
	if unresolved > 0 {
		return // retried by collectRollbacks on the next tick
	}
	b.State = domain.BatchFailed
	b.Reason = "batch rolled back after failed health gate"
	now := o.clk()
	b.DecidedAt = &now
	roll.Phase = domain.PhaseFailed
	roll.Reason = "stopped after unhealthy batch; failed batch restored to baseline"
	o.emit(mut, roll.ID, "", "ROLLOUT_FAILED", map[string]any{
		"batch": b.Index, "rolled_back": rolledBack, "rollback_failed": rbFailed,
	})
	o.log.Printf("rollout=%s FAILED at batch=%d rolled_back=%d rollback_failed=%d (healthy earlier batches retained)",
		roll.ID, b.Index, rolledBack, rbFailed)
	o.releaseLocks(mut, roll.ID)
}

// evaluateTerminating skips unstarted batches and finalizes once every device
// that reached the target image has been restored to its baseline.
func (o *orchestrator) evaluateTerminating(mut store.Mutator, roll *domain.Rollout) {
	for i := range roll.Batches {
		b := &roll.Batches[i]
		switch b.State {
		case domain.BatchPending:
			b.State = domain.BatchSkipped
			b.Reason = "rollout terminated before batch start"
			o.emit(mut, roll.ID, "", "BATCH_SKIPPED", map[string]any{"batch": i})
		case domain.BatchInProgress:
			// No gate decision was ever reached: mark the batch failed so the
			// rollout record does not masquerade an in-flight canary.
			b.State = domain.BatchFailed
			b.Deadline = nil
			b.Reason = "rollout terminated mid-batch; no gate decision"
			o.emit(mut, roll.ID, "", "BATCH_ABORTED", map[string]any{"batch": i})
		}
	}
	var unresolved, rbFailed int
	for _, p := range mut.Participants(roll.ID) {
		switch p.State {
		case domain.StateRollbackFailed:
			rbFailed++
		case domain.StateRolledBack:
			// settled
		default:
			unresolved++
		}
	}
	if unresolved > 0 {
		return
	}
	roll.Phase = domain.PhaseTerminated
	roll.Reason = "terminated; target-image devices restored to baselines"
	o.emit(mut, roll.ID, "", "ROLLOUT_TERMINATED", map[string]any{
		"rollback_failed": rbFailed,
	})
	o.log.Printf("rollout=%s TERMINATED rollback_failed=%d", roll.ID, rbFailed)
	o.releaseLocks(mut, roll.ID)
}

func (o *orchestrator) releaseLocks(mut store.Mutator, rolloutID string) {
	for _, p := range mut.Participants(rolloutID) {
		mut.UnlockDevice(p.DeviceID)
	}
}

func batchParticipants(mut store.Viewer, rolloutID string, idx int) []*domain.Participation {
	var out []*domain.Participation
	for _, p := range mut.Participants(rolloutID) {
		if p.BatchIndex == idx {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DeviceID < out[j].DeviceID })
	return out
}

// dispatch sends collected commands outside the store lock. Delivery errors
// (e.g. device offline) leave the participation in INSTALLING/ROLLING_BACK;
// deadline or retry paths on later ticks converge it, never silently dropping.
func (o *orchestrator) dispatch(ctx context.Context, rolloutID string, cmds []pendingCommand) {
	for _, c := range cmds {
		var err error
		if c.rollback {
			err = o.cmd.Rollback(ctx, c.deviceID, rolloutID, c.seq, c.from, c.target)
		} else {
			err = o.cmd.Install(ctx, c.deviceID, rolloutID, c.seq, c.from, c.target)
		}
		if err != nil {
			o.log.Printf("dispatch device=%s rollout=%s rollback=%t seq=%d error=%v (retry/timeout will converge)",
				c.deviceID, rolloutID, c.rollback, c.seq, err)
		}
	}
}

package orchestrator

import (
	"context"
	"fmt"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/store"
)

// HandleReport converges one device report.
//
// Guarantees:
//   - Unknown rollout/device reports are rejected (no state mutation).
//   - seq < last applied is a stale/duplicate report: never applied.
//   - seq == last applied is a command-result retransmission: applied only if
//     the device is still awaiting that very command; otherwise it is a
//     duplicate and the frozen conclusion is returned unchanged. This makes
//     "report lost, then retried with the same command number" converge.
//   - Once a batch has been decided (and always once the rollout is terminal)
//     install results can no longer rewrite the conclusion: only rollback
//     results are accepted while reconciliation is in progress.
//   - Terminal device states (SUCCESS / ROLLED_BACK / ROLLBACK_FAILED) are
//     immutable: a late report can never undo a finished result.
//   - A confirmed version moves the shared device.Version in the same
//     transaction as the participation transition.
func (o *orchestrator) HandleReport(ctx context.Context, r domain.Report) (ReportAck, error) {
	if r.DeviceID == "" || r.RolloutID == "" {
		return ReportAck{}, domain.ErrInvalidArgument
	}
	ack := ReportAck{}
	err := o.st.Lock(ctx, func(mut store.Mutator) error {
		roll, ok := mut.Rollout(r.RolloutID)
		if !ok {
			return fmt.Errorf("%w: rollout %s", domain.ErrNotFound, r.RolloutID)
		}
		p, ok := mut.Participation(r.RolloutID, r.DeviceID)
		if !ok {
			return fmt.Errorf("%w: device %s not in rollout %s",
				domain.ErrNotFound, r.DeviceID, r.RolloutID)
		}

		if r.Seq > 0 && r.Seq < p.LastSeq {
			ack = ReportAck{Accepted: false, State: string(p.State),
				Reason: fmt.Sprintf("stale report seq=%d < applied=%d", r.Seq, p.LastSeq)}
			return nil
		}
		if domain.TerminalDeviceStates[p.State] {
			ack = ReportAck{Accepted: false, State: string(p.State),
				Reason: "terminal conclusion is immutable"}
			return nil
		}

		// A batch decision is authoritative: an install-result arriving after
		// the gate decided is a late report and cannot rewrite the batch.
		if r.InstallOK && r.CurrentVersion == roll.TargetVersion {
			b := roll.Batches[p.BatchIndex]
			if b.State != domain.BatchInProgress && b.State != domain.BatchPending {
				ack = ReportAck{Accepted: false, State: string(p.State),
					Reason: fmt.Sprintf("batch already %s; late install report frozen", b.State)}
				return nil
			}
		}

		// Retransmission of the in-flight command result is applied once.
		if r.Seq == p.LastSeq {
			awaitingInstall := p.State == domain.StatePending || p.State == domain.StateInstalling
			awaitingRollback := p.State == domain.StateRollingBack
			versionMatches := (r.InstallOK && r.CurrentVersion == roll.TargetVersion && awaitingInstall) ||
				(r.InstallOK && r.CurrentVersion == p.FromVersion && awaitingRollback) ||
				(!r.InstallOK && (awaitingRollback || awaitingInstall))
			if !versionMatches {
				ack = ReportAck{Accepted: false, State: string(p.State),
					Reason: fmt.Sprintf("duplicate result for seq=%d in state %s", r.Seq, p.State)}
				return nil
			}
		}

		cp := *p
		cp.LastSeq = r.Seq
		cp.UpdatedAt = o.clk()
		eventType := "DEVICE_REPORT"

		switch {
		case r.InstallOK && r.CurrentVersion == roll.TargetVersion:
			if cp.State != domain.StatePending && cp.State != domain.StateInstalling {
				ack = ReportAck{Accepted: false, State: string(p.State),
					Reason: fmt.Sprintf("install success not applicable in state %s", p.State)}
				return nil
			}
			cp.State = domain.StateSuccess
			cp.Reason = fmt.Sprintf("install confirmed %s -> %s", p.FromVersion, r.CurrentVersion)
			eventType = "DEVICE_SUCCESS"
			o.bumpDeviceVersion(mut, r.DeviceID, r.CurrentVersion)
		case r.InstallOK && r.CurrentVersion == p.FromVersion:
			if cp.State != domain.StateRollingBack {
				ack = ReportAck{Accepted: false, State: string(p.State),
					Reason: fmt.Sprintf("rollback success not applicable in state %s", p.State)}
				return nil
			}
			cp.State = domain.StateRolledBack
			cp.RollbackSince = nil
			cp.Reason = fmt.Sprintf("rollback confirmed, back on known-good %s", r.CurrentVersion)
			eventType = "DEVICE_ROLLED_BACK"
			o.bumpDeviceVersion(mut, r.DeviceID, r.CurrentVersion)
		case !r.InstallOK && cp.State == domain.StateRollingBack:
			cp.State = domain.StateRollbackFailed
			cp.Reason = "rollback failed; manual intervention required"
			eventType = "DEVICE_ROLLBACK_FAILED"
		case !r.InstallOK:
			cp.State = domain.StateFailed
			cp.Attempted++
			cp.Reason = fmt.Sprintf("install failed, device stayed on %s", r.CurrentVersion)
			eventType = "DEVICE_FAILED"
		default:
			ack = ReportAck{Accepted: false, State: string(p.State),
				Reason: "report version does not match target or baseline"}
			return nil
		}

		if err := mut.SaveParticipation(&cp); err != nil {
			return err
		}
		o.emit(mut, r.RolloutID, r.DeviceID, eventType, map[string]any{
			"seq":     r.Seq,
			"state":   string(cp.State),
			"version": r.CurrentVersion,
			"ok":      r.InstallOK,
			"reason":  cp.Reason,
		})
		ack = ReportAck{Accepted: true, State: string(cp.State), Reason: cp.Reason}
		return nil
	})
	if err != nil {
		return ReportAck{}, err
	}
	o.log.Printf("report device=%s rollout=%s seq=%d ok=%t ver=%s -> accepted=%t state=%s (%s)",
		r.DeviceID, r.RolloutID, r.Seq, r.InstallOK, r.CurrentVersion, ack.Accepted, ack.State, ack.Reason)
	return ack, nil
}

// bumpDeviceVersion records the newly confirmed firmware version. Callers only
// invoke this for a verified success/rollback event belonging to the device's
// locked rollout, so a bare or foreign report can never move the version.
func (o *orchestrator) bumpDeviceVersion(mut store.Mutator, id, version string) {
	if d, ok := mut.Device(id); ok {
		nd := *d
		nd.Version = version
		nd.Online = true
		nd.UpdatedAt = o.clk()
		_ = mut.UpsertDevice(&nd)
	}
}

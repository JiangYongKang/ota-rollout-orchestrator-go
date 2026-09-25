package orchestrator

import (
	"context"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/store"
)

// PauseRollout suspends batch promotion. Reports from in-flight devices keep
// being applied; only promotion/dispatch decisions stop.
func (o *orchestrator) PauseRollout(ctx context.Context, id string) (*domain.Rollout, error) {
	return o.transition(ctx, id, "PAUSE", func(r *domain.Rollout) bool {
		// Pending (not yet started) and running rollouts are both pausable.
		if r.Phase != domain.PhaseRunning && r.Phase != domain.PhasePending {
			return false
		}
		r.Phase = domain.PhasePaused
		r.Reason = "paused by operator"
		return true
	})
}

// ResumeRollout continues a paused rollout.
func (o *orchestrator) ResumeRollout(ctx context.Context, id string) (*domain.Rollout, error) {
	return o.transition(ctx, id, "RESUME", func(r *domain.Rollout) bool {
		if r.Phase != domain.PhasePaused {
			return false
		}
		r.Phase = domain.PhaseRunning
		r.Reason = "resumed by operator"
		return true
	})
}

// TerminateRollout requests termination. Devices already on the target
// version are rolled back to their captured baselines asynchronously by Tick;
// unstarted batches are skipped. The call is idempotent.
func (o *orchestrator) TerminateRollout(ctx context.Context, id string) (*domain.Rollout, error) {
	return o.transition(ctx, id, "TERMINATE", func(r *domain.Rollout) bool {
		switch r.Phase {
		case domain.PhasePending, domain.PhaseRunning, domain.PhasePaused:
			r.Phase = domain.PhaseTerminating
			r.Reason = "terminated by operator; rollback pending"
			return true
		default:
			return false
		}
	})
}

func (o *orchestrator) transition(ctx context.Context, id, action string,
	apply func(r *domain.Rollout) bool) (*domain.Rollout, error) {

	var saved *domain.Rollout
	err := o.st.Lock(ctx, func(mut store.Mutator) error {
		r, ok := mut.Rollout(id)
		if !ok {
			return domain.ErrNotFound
		}
		cp := *r
		if !apply(&cp) {
			return domain.ErrConflict
		}
		cp.UpdatedAt = o.clk()
		if err := mut.SaveRollout(&cp); err != nil {
			return err
		}
		o.emit(mut, id, "", "ROLLOUT_"+action, map[string]any{
			"phase": string(cp.Phase), "reason": cp.Reason,
		})
		saved = &cp
		return nil
	})
	if err != nil {
		return nil, err
	}
	o.log.Printf("rollout=%s action=%s phase=%s", id, action, saved.Phase)
	return saved, nil
}

package orchestrator

import (
	"context"
	"fmt"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/store"
)

// CreateRollout creates a batched canary rollout.
//
// Each target device's current confirmed version is captured as its rollback
// baseline, and the device is locked to this rollout for its whole duration so
// concurrent rollouts can never interleave firmware on the same target.
func (o *orchestrator) CreateRollout(ctx context.Context, req CreateRequest) (*domain.Rollout, error) {
	if req.ID == "" || req.TargetVersion == "" || len(req.DeviceIDs) == 0 {
		return nil, domain.ErrInvalidArgument
	}
	ratio := o.cfg.SuccessRatio
	if req.SuccessRatio != nil {
		ratio = *req.SuccessRatio
	}
	if ratio <= 0 || ratio > 1 {
		return nil, fmt.Errorf("%w: success ratio must be in (0,1]", domain.ErrInvalidArgument)
	}

	// Deterministic device order -> deterministic batch membership.
	ids := dedupSorted(req.DeviceIDs)
	sizes := normalizeBatchSizes(req.BatchSizes, len(ids))

	var saved *domain.Rollout
	err := o.st.Lock(ctx, func(mut store.Mutator) error {
		if _, exists := mut.Rollout(req.ID); exists {
			return fmt.Errorf("%w: rollout %s", domain.ErrAlreadyExists, req.ID)
		}
		// Validate all devices up front and acquire locks before any write of
		// consequence, so two concurrent create calls can never both partly win.
		baselines := make(map[string]string, len(ids))
		for _, id := range ids {
			d, ok := mut.Device(id)
			if !ok {
				return fmt.Errorf("%w: device %s", domain.ErrNotFound, id)
			}
			if d.Version == req.TargetVersion {
				return fmt.Errorf("%w: device %s already on %s",
					domain.ErrInvalidArgument, id, req.TargetVersion)
			}
			if err := mut.LockDevice(id, req.ID); err != nil {
				return err
			}
			baselines[id] = d.Version
		}

		now := o.clk()
		r := &domain.Rollout{
			ID:            req.ID,
			TargetVersion: req.TargetVersion,
			SuccessRatio:  ratio,
			Cursor:        0,
			Phase:         domain.PhasePending,
			BatchSizes:    sizes,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		offset := 0
		for i, sz := range sizes {
			r.Batches = append(r.Batches, domain.Batch{
				Index: i, Size: sz, State: domain.BatchPending,
			})
			for j := 0; j < sz; j++ {
				id := ids[offset+j]
				if err := mut.SaveParticipation(&domain.Participation{
					RolloutID:     req.ID,
					DeviceID:      id,
					BatchIndex:    i,
					State:         domain.StatePending,
					FromVersion:   baselines[id],
					TargetVersion: req.TargetVersion,
					Reason:        "targeted at rollout creation",
					UpdatedAt:     now,
				}); err != nil {
					return err
				}
			}
			offset += sz
		}
		if err := mut.SaveRollout(r); err != nil {
			return err
		}
		o.emit(mut, req.ID, "", "ROLLOUT_CREATED", map[string]any{
			"target":  req.TargetVersion,
			"devices": len(ids),
			"batches": len(sizes),
			"ratio":   ratio,
		})
		saved = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	o.log.Printf("rollout=%s created target=%s devices=%d batches=%v ratio=%.2f",
		req.ID, req.TargetVersion, len(ids), sizes, ratio)
	return saved, nil
}

func dedupSorted(in []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	sortStrings(out)
	return out
}

func sortStrings(a []string) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j-1] > a[j]; j-- {
			a[j-1], a[j] = a[j], a[j-1]
		}
	}
}

// normalizeBatchSizes turns an arbitrary size hint into sizes covering n.
// Empty hint -> single batch; overshoot is trimmed; shortfall is appended.
func normalizeBatchSizes(hint []int, n int) []int {
	var sizes []int
	used := 0
	for _, sz := range hint {
		if sz <= 0 || used >= n {
			continue
		}
		if used+sz > n {
			sz = n - used
		}
		sizes = append(sizes, sz)
		used += sz
	}
	if used < n {
		sizes = append(sizes, n-used)
	}
	return sizes
}

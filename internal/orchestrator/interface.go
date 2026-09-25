package orchestrator

import (
	"context"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/fleet"
)

// Orchestrator is the public rollout-management surface.
type Orchestrator interface {
	Start(ctx context.Context) error
	Shutdown(ctx context.Context) error

	UpsertDevice(ctx context.Context, id, version string) (*domain.Device, error)
	RemoveDevice(ctx context.Context, id string) error
	Devices(ctx context.Context) ([]*domain.Device, error)
	SetDeviceOnline(ctx context.Context, id string, online bool) error
	SetDeviceFault(ctx context.Context, id string, fault fleet.Fault) error

	CreateRollout(ctx context.Context, req CreateRequest) (*domain.Rollout, error)
	PauseRollout(ctx context.Context, id string) (*domain.Rollout, error)
	ResumeRollout(ctx context.Context, id string) (*domain.Rollout, error)
	TerminateRollout(ctx context.Context, id string) (*domain.Rollout, error)
	Rollout(ctx context.Context, id string) (*domain.Rollout, error)
	Rollouts(ctx context.Context) ([]*domain.Rollout, error)
	Participation(ctx context.Context, rolloutID, deviceID string) (*domain.Participation, error)
	Participants(ctx context.Context, rolloutID string) ([]*domain.Participation, error)
	Events(ctx context.Context, rolloutID string) ([]*domain.Event, error)

	HandleReport(ctx context.Context, r domain.Report) (ReportAck, error)
	Tick(ctx context.Context) error
}

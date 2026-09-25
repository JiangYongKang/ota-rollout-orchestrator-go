package orchestrator

import (
	"context"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/fleet"
)

// fleetController is optionally implemented by the Commander for simulation
// control: taking devices offline/reconnecting and injecting install faults.
type fleetController interface {
	SetOnline(id string, online bool)
	SetFault(id string, fault fleet.Fault)
}

// SetDeviceOnline simulates a device going offline or reconnecting.
func (o *orchestrator) SetDeviceOnline(ctx context.Context, id string, online bool) error {
	fc, ok := o.cmd.(fleetController)
	if !ok {
		return nil
	}
	fc.SetOnline(id, online)
	return nil
}

// SetDeviceFault injects a simulated behavior for a device.
func (o *orchestrator) SetDeviceFault(ctx context.Context, id string, fault fleet.Fault) error {
	fc, ok := o.cmd.(fleetController)
	if !ok {
		return nil
	}
	fc.SetFault(id, fault)
	return nil
}

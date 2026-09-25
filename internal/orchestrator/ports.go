package orchestrator

// deviceManager is optionally implemented by the Commander (the simulated
// fleet implements it) so device registry changes propagate to devices.
type deviceManager interface {
	AddDevice(id, version string)
	RemoveDevice(id string)
}

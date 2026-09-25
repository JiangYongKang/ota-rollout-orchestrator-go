// Package domain defines the core state model for the local OTA rollout
// orchestrator: devices, rollouts, batches, participation records and events.
package domain

import "time"

// DeviceState is the lifecycle state of a device within one rollout.
type DeviceState string

const (
	StatePending        DeviceState = "PENDING"    // targeted, install not yet acknowledged
	StateInstalling     DeviceState = "INSTALLING" // install command acknowledged
	StateSuccess        DeviceState = "SUCCESS"    // reported upgraded to target version
	StateFailed         DeviceState = "FAILED"     // reported install failure
	StateOffline        DeviceState = "OFFLINE"    // expected report never arrived
	StateRollingBack    DeviceState = "ROLLING_BACK"
	StateRolledBack     DeviceState = "ROLLED_BACK"     // back on known-good baseline
	StateRollbackFailed DeviceState = "ROLLBACK_FAILED" // needs manual handling
	StateUntargeted     DeviceState = "UNTARGETED"      // device not part of batch
)

// TerminalDeviceStates never change for a given rollout once reached.
var TerminalDeviceStates = map[DeviceState]bool{
	StateSuccess:        true,
	StateRolledBack:     true,
	StateRollbackFailed: true,
}

// RolloutPhase is the lifecycle phase of a rollout task.
type RolloutPhase string

const (
	PhasePending     RolloutPhase = "PENDING"
	PhaseRunning     RolloutPhase = "RUNNING"
	PhasePaused      RolloutPhase = "PAUSED"
	PhaseFailing     RolloutPhase = "FAILING"     // batch failed health gate; rolling back that batch
	PhaseFailed      RolloutPhase = "FAILED"      // stopped due to health failure (failed batch isolated)
	PhaseTerminating RolloutPhase = "TERMINATING" // user requested stop; rolling back all
	PhaseTerminated  RolloutPhase = "TERMINATED"
	PhaseCompleted   RolloutPhase = "COMPLETED"
)

// BatchState is the state of an individual batch.
type BatchState string

const (
	BatchPending    BatchState = "PENDING"
	BatchInProgress BatchState = "IN_PROGRESS"
	BatchHealthy    BatchState = "HEALTHY" // success ratio met, gate passed
	BatchFailing    BatchState = "FAILING" // rolled back because gate failed
	BatchFailed     BatchState = "FAILED"
	BatchSkipped    BatchState = "SKIPPED"   // rollout terminated before start
	BatchCompleted  BatchState = "COMPLETED" // finished as part of rollout completion
)

type Device struct {
	ID        string    `json:"id"`
	Version   string    `json:"version"` // currently confirmed firmware version
	Online    bool      `json:"online"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Participation is a device's immutable-per-conclusion record in one rollout.
type Participation struct {
	RolloutID     string      `json:"rollout_id"`
	DeviceID      string      `json:"device_id"`
	BatchIndex    int         `json:"batch_index"`
	State         DeviceState `json:"state"`
	FromVersion   string      `json:"from_version"` // baseline captured at creation
	TargetVersion string      `json:"target_version"`
	LastSeq       int64       `json:"last_seq"` // last applied report seq (dedup/ordering)
	Reason        string      `json:"reason"`   // human-meaningful explanation
	Attempted     int         `json:"attempted"`
	RollbackSince *time.Time  `json:"rollback_since,omitempty"`
	UpdatedAt     time.Time   `json:"updated_at"`
}

type Batch struct {
	Index     int        `json:"index"`
	Size      int        `json:"size"`
	State     BatchState `json:"state"`
	Success   int        `json:"success"`
	Failed    int        `json:"failed"`
	Offline   int        `json:"offline"`
	Required  int        `json:"required"` // success ratio gate threshold count
	Reason    string     `json:"reason"`
	StartedAt *time.Time `json:"started_at,omitempty"`
	DecidedAt *time.Time `json:"decided_at,omitempty"`
	Deadline  *time.Time `json:"deadline,omitempty"` // gate decision deadline
}

type Rollout struct {
	ID            string       `json:"id"`
	TargetVersion string       `json:"target_version"`
	BatchSizes    []int        `json:"batch_sizes"`
	SuccessRatio  float64      `json:"success_ratio"`
	Cursor        int          `json:"cursor"` // index of current/last touched batch
	Phase         RolloutPhase `json:"phase"`
	Reason        string       `json:"reason"`
	Batches       []Batch      `json:"batches"`
	CreatedAt     time.Time    `json:"created_at"`
	UpdatedAt     time.Time    `json:"updated_at"`
}

// Report is a device -> orchestrator status report.
type Report struct {
	DeviceID       string `json:"device_id"`
	RolloutID      string `json:"rollout_id"`
	Seq            int64  `json:"seq"`
	FromVersion    string `json:"from_version"`
	CurrentVersion string `json:"current_version"`
	InstallOK      bool   `json:"install_ok"`
	Online         bool   `json:"online"`
}

// Event records every meaningful state transition for audit/explainability.
type Event struct {
	Seq       int64          `json:"seq"`
	Time      time.Time      `json:"time"`
	RolloutID string         `json:"rollout_id,omitempty"`
	DeviceID  string         `json:"device_id,omitempty"`
	Type      string         `json:"type"`
	Detail    map[string]any `json:"detail,omitempty"`
}

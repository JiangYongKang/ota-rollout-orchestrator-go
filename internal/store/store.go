// Package store provides the local, process-restart-stable state store.
//
// The whole state is kept in a single JSON snapshot that is atomically
// rewritten on every committed mutation (temp file + rename), fsync'ing the
// directory afterwards. All mutations are serialized by one RWMutex, which
// gives the orchestrator simple transactional semantics: a mutation either
// commits (and is persisted) or returns an error without touching state.
//
// Viewer reads return *deep copies*: callers (e.g. HTTP JSON encoding) may
// use them after the read lock is released without racing background ticks.
// Mutators intentionally hand out live pointers for use *inside* the write
// transaction only.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/domain"
)

// Store is the persistence abstraction.
type Store interface {
	// Lock runs fn while holding the write lock; state is persisted iff fn
	// returns nil. Returning an error aborts the (in-memory) transaction.
	Lock(ctx context.Context, fn func(mut Mutator) error) error
	// View runs fn while holding the read lock; values exposed by Viewer are
	// deep copies and safe to retain past the callback.
	View(ctx context.Context, fn func(v Viewer) error) error
	Close() error
}

type Mutator interface {
	Viewer
	UpsertDevice(d *domain.Device) error
	DeleteDevice(id string) error
	SaveRollout(r *domain.Rollout) error
	SaveParticipation(p *domain.Participation) error
	AppendEvent(e *domain.Event) int64
	LockDevice(deviceID, rolloutID string) error
	UnlockDevice(deviceID string)
}

type Viewer interface {
	Device(id string) (*domain.Device, bool)
	Devices() []*domain.Device
	Rollout(id string) (*domain.Rollout, bool)
	Rollouts() []*domain.Rollout
	Participation(rolloutID, deviceID string) (*domain.Participation, bool)
	Participants(rolloutID string) []*domain.Participation
	DeviceLock(deviceID string) (string, bool)
	Events(rolloutID string) []*domain.Event
	NextEventSeq() int64
}

type snapshot struct {
	Devices        map[string]*domain.Device        `json:"devices"`
	Rollouts       map[string]*domain.Rollout       `json:"rollouts"`
	Participations map[string]*domain.Participation `json:"participations"`
	DeviceLocks    map[string]string                `json:"device_locks"`
	Events         []*domain.Event                  `json:"events"`
	EventSeq       int64                            `json:"event_seq"`
}

type fileStore struct {
	path string
	mu   sync.RWMutex
	snap *snapshot
}

// New opens (or creates) the store backed by path.
func New(path string) (Store, error) {
	fs := &fileStore{path: path, snap: newSnapshot()}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if len(data) > 0 {
			if err := json.Unmarshal(data, fs.snap); err != nil {
				return nil, fmt.Errorf("load state: %w", err)
			}
			fs.snap.normalize()
		}
	case os.IsNotExist(err):
		// empty start
	default:
		return nil, err
	}
	return fs, nil
}

func newSnapshot() *snapshot {
	return &snapshot{
		Devices:        map[string]*domain.Device{},
		Rollouts:       map[string]*domain.Rollout{},
		Participations: map[string]*domain.Participation{},
		DeviceLocks:    map[string]string{},
	}
}

func (s *snapshot) normalize() {
	if s.Devices == nil {
		s.Devices = map[string]*domain.Device{}
	}
	if s.Rollouts == nil {
		s.Rollouts = map[string]*domain.Rollout{}
	}
	if s.Participations == nil {
		s.Participations = map[string]*domain.Participation{}
	}
	if s.DeviceLocks == nil {
		s.DeviceLocks = map[string]string{}
	}
}

func pkey(rolloutID, deviceID string) string {
	return rolloutID + "|" + deviceID
}

// clone is a generic JSON deep copy used for read-returned values.
func clone[T any](in *T) *T {
	data, err := json.Marshal(in)
	if err != nil {
		// All snapshot types are plain JSON-serializable structs.
		panic(fmt.Sprintf("store clone marshal: %v", err))
	}
	var out T
	if err := json.Unmarshal(data, &out); err != nil {
		panic(fmt.Sprintf("store clone unmarshal: %v", err))
	}
	return &out
}

func cloneSlice[T any](in []*T) []*T {
	out := make([]*T, 0, len(in))
	for _, v := range in {
		out = append(out, clone(v))
	}
	return out
}

func (fs *fileStore) Lock(ctx context.Context, fn func(mut Mutator) error) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if err := fn(fs); err != nil {
		return err
	}
	if err := fs.persistLocked(); err != nil {
		return fmt.Errorf("persist state: %w", err)
	}
	return nil
}

func (fs *fileStore) View(ctx context.Context, fn func(v Viewer) error) error {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fn(fs)
}

func (fs *fileStore) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.persistLocked()
}

// ---------- Mutator ----------

func (fs *fileStore) UpsertDevice(d *domain.Device) error {
	if d == nil || d.ID == "" {
		return domain.ErrInvalidArgument
	}
	fs.snap.Devices[d.ID] = clone(d)
	return nil
}

func (fs *fileStore) DeleteDevice(id string) error {
	if _, ok := fs.snap.Devices[id]; !ok {
		return domain.ErrNotFound
	}
	delete(fs.snap.Devices, id)
	return nil
}

func (fs *fileStore) SaveRollout(r *domain.Rollout) error {
	if r == nil || r.ID == "" {
		return domain.ErrInvalidArgument
	}
	fs.snap.Rollouts[r.ID] = clone(r)
	return nil
}

func (fs *fileStore) SaveParticipation(p *domain.Participation) error {
	if p == nil || p.RolloutID == "" || p.DeviceID == "" {
		return domain.ErrInvalidArgument
	}
	fs.snap.Participations[pkey(p.RolloutID, p.DeviceID)] = clone(p)
	return nil
}

func (fs *fileStore) AppendEvent(e *domain.Event) int64 {
	fs.snap.EventSeq++
	e.Seq = fs.snap.EventSeq
	fs.snap.Events = append(fs.snap.Events, clone(e))
	return e.Seq
}

func (fs *fileStore) LockDevice(deviceID, rolloutID string) error {
	if owner, ok := fs.snap.DeviceLocks[deviceID]; ok && owner != rolloutID {
		return fmt.Errorf("%w: device %s locked by rollout %s",
			domain.ErrDeviceBusy, deviceID, owner)
	}
	fs.snap.DeviceLocks[deviceID] = rolloutID
	return nil
}

func (fs *fileStore) UnlockDevice(deviceID string) { delete(fs.snap.DeviceLocks, deviceID) }

// ---------- Viewer (deep copies) ----------

func (fs *fileStore) Device(id string) (*domain.Device, bool) {
	d, ok := fs.snap.Devices[id]
	if !ok {
		return nil, false
	}
	return clone(d), true
}

func (fs *fileStore) Devices() []*domain.Device {
	out := make([]*domain.Device, 0, len(fs.snap.Devices))
	for _, d := range fs.snap.Devices {
		out = append(out, clone(d))
	}
	return out
}

func (fs *fileStore) Rollout(id string) (*domain.Rollout, bool) {
	r, ok := fs.snap.Rollouts[id]
	if !ok {
		return nil, false
	}
	return clone(r), true
}

func (fs *fileStore) Rollouts() []*domain.Rollout {
	out := make([]*domain.Rollout, 0, len(fs.snap.Rollouts))
	for _, r := range fs.snap.Rollouts {
		out = append(out, clone(r))
	}
	return out
}

func (fs *fileStore) Participation(rolloutID, deviceID string) (*domain.Participation, bool) {
	p, ok := fs.snap.Participations[pkey(rolloutID, deviceID)]
	if !ok {
		return nil, false
	}
	return clone(p), true
}

func (fs *fileStore) Participants(rolloutID string) []*domain.Participation {
	var out []*domain.Participation
	for k, p := range fs.snap.Participations {
		if strings.HasPrefix(k, rolloutID+"|") {
			out = append(out, clone(p))
		}
	}
	return out
}

func (fs *fileStore) DeviceLock(deviceID string) (string, bool) {
	owner, ok := fs.snap.DeviceLocks[deviceID]
	return owner, ok
}

func (fs *fileStore) Events(rolloutID string) []*domain.Event {
	var out []*domain.Event
	for _, e := range fs.snap.Events {
		if rolloutID == "" || e.RolloutID == rolloutID {
			out = append(out, clone(e))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

func (fs *fileStore) NextEventSeq() int64 { return fs.snap.EventSeq + 1 }

// ---------- persistence ----------

func (fs *fileStore) persistLocked() error {
	data, err := json.MarshalIndent(fs.snap, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(fs.path)
	tmp, err := os.CreateTemp(dir, ".state-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, fs.path); err != nil {
		os.Remove(tmpName)
		return err
	}
	// Best-effort directory fsync so the rename is durable.
	if df, err := os.Open(dir); err == nil {
		_ = df.Sync()
		_ = df.Close()
	}
	return nil
}

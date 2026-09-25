package ota

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

// Store 本地状态存储，JSON 文件持久化，进程重启后已确认结论保持稳定。
type Store struct {
	mu      sync.Mutex
	path    string
	Tasks   map[string]*Task   `json:"tasks"`
	Devices map[string]*Device `json:"devices"`
	Events  []Event            `json:"events"`
}

// NewStore 打开（或创建）位于 path 的本地状态存储。
func NewStore(path string) (*Store, error) {
	s := &Store{
		path:    path,
		Tasks:   map[string]*Task{},
		Devices: map[string]*Device{},
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, s); err != nil {
			return nil, err
		}
	}
	if s.Tasks == nil {
		s.Tasks = map[string]*Task{}
	}
	if s.Devices == nil {
		s.Devices = map[string]*Device{}
	}
	return s, nil
}

// Save 将当前状态原子写盘（临时文件 + rename）。
func (s *Store) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked()
}

func (s *Store) saveLocked() error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// AddEvent 追加一条状态变更事件并持久化。
func (s *Store) AddEvent(e Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Events = append(s.Events, e)
	_ = s.saveLocked()
}

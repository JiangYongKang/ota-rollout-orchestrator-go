package ota

import (
	"bufio"
	"encoding/json"
	"errors"
	"hash/fnv"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// deviceShardCount 设备状态分片数（固定）。设备按 ID 哈希落片，
// 单笔设备上报只需原子重写约 1/devicesShardCount 大小的一个分片，
// 使上报落盘成本与设备总量解耦，满足千台规模的吞吐要求。
const deviceShardCount = 16

// Store 本地状态存储，持久化拆分为三类文件：
//   - 主 JSON（path）：仅任务状态，原子写盘（tmp + rename）；
//   - 设备分片（path + ".d" + 两位片号）：每片一部分设备，仅重写被改动的片；
//   - 事件日志（path + ".events.jsonl"）：每行一条事件，仅追加（O(1)）。
//
// 加载时兼容历史版本：旧版把 devices/events 内嵌在主 JSON 中，首次加载自动迁移。
type Store struct {
	mu         sync.Mutex
	path       string
	eventsPath string
	Tasks      map[string]*Task   `json:"tasks"`
	Devices    map[string]*Device `json:"-"` // 内存聚合视图，按分片持久化
	Events     []Event            `json:"-"` // 事件存于 eventsPath，仅追加

	shards      []map[string]*Device // 分片内存视图
	dirtyShards map[int]bool         // 本次 Save 需要重写的分片
	eventFile   *os.File             // 长开的 append-only 事件日志句柄
}

// storeLoad 仅用于兼容读取旧版（devices/events 内嵌主 JSON）的文件。
type storeLoad struct {
	Tasks   map[string]*Task   `json:"tasks"`
	Devices map[string]*Device `json:"devices"`
	Events  []Event            `json:"events"`
}

// NewStore 打开（或创建）位于 path 的本地状态存储。
func NewStore(path string) (*Store, error) {
	s := &Store{
		path:        path,
		eventsPath:  path + ".events.jsonl",
		Tasks:       map[string]*Task{},
		Devices:     map[string]*Device{},
		shards:      make([]map[string]*Device, deviceShardCount),
		dirtyShards: map[int]bool{},
	}
	for i := range s.shards {
		s.shards[i] = map[string]*Device{}
	}

	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	legacyDevices := map[string]*Device{}
	legacyEvents := []Event(nil)
	if len(data) > 0 {
		var old storeLoad
		if err := json.Unmarshal(data, &old); err != nil {
			return nil, err
		}
		s.Tasks = old.Tasks
		if s.Tasks == nil {
			s.Tasks = map[string]*Task{}
		}
		legacyDevices = old.Devices
		legacyEvents = old.Events
	}

	if len(legacyDevices) > 0 {
		// 旧版内嵌设备：先尝试从分片加载；若分片不存在则迁移。
		loadedAny := s.loadDeviceShardsLocked()
		if !loadedAny {
			for _, d := range legacyDevices {
				s.addDeviceLocked(d)
			}
		} else {
			for _, d := range legacyDevices {
				s.attachDeviceLocked(d)
			}
		}
	} else {
		s.loadDeviceShardsLocked()
	}

	// 事件：优先读 JSONL；不存在时把旧文件内嵌事件迁移一次。
	if _, statErr := os.Stat(s.eventsPath); errors.Is(statErr, os.ErrNotExist) && len(legacyEvents) > 0 {
		if err := s.migrateEventsLocked(legacyEvents); err != nil {
			return nil, err
		}
	}
	if err := s.loadEventsLocked(); err != nil {
		return nil, err
	}

	ef, err := os.OpenFile(s.eventsPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.eventFile = ef
	return s, nil
}

// Close 刷新脏分片并关闭事件日志句柄。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for idx := range s.dirtyShards {
		if err := s.writeShardLocked(idx); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(s.dirtyShards, idx)
	}
	if s.eventFile != nil {
		if err := s.eventFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.eventFile = nil
	}
	return firstErr
}

func shardIndexFor(id string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % deviceShardCount)
}

func (s *Store) shardPath(idx int) string {
	return s.path + ".d" + strconv.Itoa(idx)
}

// addDeviceLocked 新建设备：登记聚合视图、分片并标记脏片。
func (s *Store) addDeviceLocked(d *Device) {
	s.attachDeviceLocked(d)
	s.dirtyShards[shardIndexFor(d.ID)] = true
}

// AddDevice 线程安全地登记一台新设备。
func (s *Store) AddDevice(d *Device) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addDeviceLocked(d)
}

// attachDeviceLocked 仅登记内存视图（用于加载/迁移），不标记脏。
func (s *Store) attachDeviceLocked(d *Device) {
	idx := shardIndexFor(d.ID)
	s.Devices[d.ID] = d
	s.shards[idx][d.ID] = d
}

// touchDevice 标记一台已变更设备所在分片为脏（调用方持有编排器锁即可）。
func (s *Store) touchDevice(d *Device) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirtyShards[shardIndexFor(d.ID)] = true
}

// loadDeviceShardsLocked 从分片文件读回设备；返回是否读到了任意分片。
func (s *Store) loadDeviceShardsLocked() bool {
	loadedAny := false
	for idx := 0; idx < deviceShardCount; idx++ {
		raw, err := os.ReadFile(s.shardPath(idx))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			continue
		}
		if len(raw) == 0 {
			continue
		}
		var m map[string]*Device
		if err := json.Unmarshal(raw, &m); err != nil {
			continue
		}
		loadedAny = true
		for id, d := range m {
			s.Devices[id] = d
			s.shards[idx][id] = d
		}
	}
	return loadedAny
}

func (s *Store) writeShardLocked(idx int) error {
	data, err := json.MarshalIndent(s.shards[idx], "", "  ")
	if err != nil {
		return err
	}
	p := s.shardPath(idx)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

// migrateEventsLocked 把旧文件内嵌事件一次性写入 JSONL。
func (s *Store) migrateEventsLocked(events []Event) error {
	if err := os.MkdirAll(filepath.Dir(s.eventsPath), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(s.eventsPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for i := range events {
		if err := enc.Encode(&events[i]); err != nil {
			return err
		}
	}
	return w.Flush()
}

// loadEventsLocked 从 JSONL 读回全部事件（重启后追溯恢复）。
func (s *Store) loadEventsLocked() error {
	f, err := os.Open(s.eventsPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e Event
		if err := json.Unmarshal(line, &e); err != nil {
			return err
		}
		s.Events = append(s.Events, e)
	}
	return sc.Err()
}

// Save 将任务状态与所有脏设备分片原子写盘；事件已在产生时追加，不在此处理。
func (s *Store) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveStateLocked()
}

func (s *Store) saveStateLocked() error {
	taskData, err := json.MarshalIndent(struct {
		Tasks map[string]*Task `json:"tasks"`
	}{Tasks: s.Tasks}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, taskData, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	for idx := range s.dirtyShards {
		if err := s.writeShardLocked(idx); err != nil {
			return err
		}
		delete(s.dirtyShards, idx)
	}
	return nil
}

// AppendEvent 追加一条事件到内存与 JSONL 日志（O(1)，不重写状态文件）。
func (s *Store) AppendEvent(e Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Events = append(s.Events, e)
	return s.appendEventLineLocked(e)
}

func (s *Store) appendEventLineLocked(e Event) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if s.eventFile == nil {
		return errors.New("事件日志句柄未初始化")
	}
	_, err = s.eventFile.Write(data)
	return err
}

// AddEvent 追加一条状态变更事件并持久化（保留对外便捷接口）。
func (s *Store) AddEvent(e Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Events = append(s.Events, e)
	_ = s.appendEventLineLocked(e)
}

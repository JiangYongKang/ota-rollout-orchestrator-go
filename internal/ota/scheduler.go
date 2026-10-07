package ota

import (
	"log"
	"sync"
	"time"
)

// Scheduler 批次观察期自动调度器：为每个处于观察中的任务维护一个到点定时器，
// 到点后以自动触发来源对当前批次做且仅做一次健康判定。
// 定时器生命周期与任务状态联动，多个任务的定时器互不影响。
//
// 加锁顺序固定为：Orchestrator.mu -> Scheduler.mu，避免与回调路径形成锁环。
type Scheduler struct {
	o      *Orchestrator
	mu     sync.Mutex
	timers map[string]*time.Timer // taskID -> 当前批次的待触发定时器
	closed bool                   // Stop 之后不再接受新定时器/回调
	wg     sync.WaitGroup         // 等待在途回调退出，保证 Stop 后不再触碰状态
}

func newScheduler(o *Orchestrator) *Scheduler {
	return &Scheduler{o: o, timers: map[string]*time.Timer{}}
}

// restore 进程重启后恢复：为仍在观察中的任务按持久化的截止时刻重建定时器；
// 已过截止时刻的立即补评估（自动触发），已形成结论的批次不重建，结论不重复。
func (s *Scheduler) restore() {
	s.o.mu.Lock()
	defer s.o.mu.Unlock()
	now := s.o.now()
	for id, task := range s.o.store.Tasks {
		if task.Status != TaskRolling || task.CurrentBatch < 0 {
			continue
		}
		batch := &task.Batches[task.CurrentBatch]
		if batch.Decided || batch.ObserveDeadline == nil || task.ObservePeriod <= 0 {
			continue
		}
		remaining := batch.ObserveDeadline.Sub(now)
		if remaining <= 0 {
			// 观察期在停机期间已到点：立即补一次自动评估，事件标注为重启补偿。
			s.o.eventLocked(id, "", "observe_due_restore",
				"观察期在停机期间到点，重启后立即补评估")
			s.o.evaluateTriggerLocked(id, TriggerAuto)
			continue
		}
		s.armLocked(task)
	}
}

// Stop 取消全部待触发定时器并等待在途回调结束；返回后不会再产生自动评估。
func (s *Scheduler) Stop() {
	s.mu.Lock()
	s.closed = true
	for id, t := range s.timers {
		t.Stop()
		delete(s.timers, id)
	}
	s.mu.Unlock()
	s.wg.Wait()
}

// armLocked 按当前批次持久化的观察截止时刻重建（或替换）该任务的定时器。
// 调用方必须持有 Orchestrator.mu。
func (s *Scheduler) armLocked(task *Task) {
	if task.ObservePeriod <= 0 || task.CurrentBatch < 0 {
		return
	}
	batch := &task.Batches[task.CurrentBatch]
	if batch.Decided || batch.ObserveDeadline == nil {
		return
	}
	remaining := batch.ObserveDeadline.Sub(s.o.now())
	if remaining < 0 {
		remaining = 0
	}
	id := task.ID
	fire := func() {
		s.wg.Add(1)
		defer s.wg.Done()
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		// 触发即摘除：同一批次的定时器至多有一个回调能真正进入评估。
		if t, ok := s.timers[id]; ok {
			t.Stop()
			delete(s.timers, id)
		}
		s.mu.Unlock()
		s.fire(id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if old, ok := s.timers[id]; ok {
		old.Stop()
	}
	s.timers[id] = time.AfterFunc(remaining, fire)
}

// disarmLocked 取消某任务的待触发定时器（暂停/终止/提前判定时调用）。
// 调用方必须持有 Orchestrator.mu。
func (s *Scheduler) disarmLocked(taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.timers[taskID]; ok {
		t.Stop()
		delete(s.timers, taskID)
	}
}

// fire 观察期到点回调：以自动触发来源评估任务当前批次。
// 幂等与一次性结论由 evaluateTriggerLocked 在编排器锁内最终保证。
func (s *Scheduler) fire(taskID string) {
	s.o.mu.Lock()
	defer s.o.mu.Unlock()
	task, err := s.o.taskLocked(taskID)
	if err != nil {
		return
	}
	if task.Status != TaskRolling || task.CurrentBatch < 0 {
		return
	}
	batch := &task.Batches[task.CurrentBatch]
	if batch.Decided {
		return
	}
	log.Printf("[ota-scheduler] 观察期到点 自动评估触发 task=%s batch=%d", taskID, batch.Index)
	s.o.evaluateTriggerLocked(taskID, TriggerAuto)
}

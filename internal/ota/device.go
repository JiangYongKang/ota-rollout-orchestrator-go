package ota

import "sync"

// SimDevice 模拟设备：接收下发、产生上报，可注入离线/故障。
// 设备本地维护单调递增序号，重连后补发未确认的上报。
type SimDevice struct {
	mu      sync.Mutex
	ID      string
	Version string // 设备当前实际版本
	Online  bool
	seq     uint64

	// 故障注入开关
	FailNextUpgrade bool // 下一次升级失败
	DropReports     bool // 模拟离线上报缺失：产生上报但不发送

	pendingTask   string // 已接收但未完成上报的任务
	pendingTarget string
}

// NewSimDevice 创建模拟设备。
func NewSimDevice(id, version string) *SimDevice {
	return &SimDevice{ID: id, Version: version, Online: true}
}

// Deliver 接收一次升级下发，返回设备将产生的上报（离线或 DropReports 时为空）。
// 重复下发同一任务同一目标版本为幂等：已处于该版本则直接回报成功，不重复安装。
func (d *SimDevice) Deliver(taskID, targetVersion string) []Report {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.Online || d.DropReports {
		return nil
	}
	// 幂等：设备已是目标版本，直接回报成功，不重复安装
	if d.Version == targetVersion {
		return []Report{
			d.reportLocked(taskID, ReportAck, ""),
			d.reportLocked(taskID, ReportSuccess, ""),
		}
	}
	reports := []Report{d.reportLocked(taskID, ReportAck, "")}
	if d.FailNextUpgrade {
		d.FailNextUpgrade = false
		reports = append(reports, d.failReportLocked(taskID, "注入故障：刷写失败"))
		return reports
	}
	d.Version = targetVersion
	d.pendingTask = taskID
	d.pendingTarget = targetVersion
	reports = append(reports, d.reportLocked(taskID, ReportSuccess, ""))
	return reports
}

// GoOffline 模拟设备离线。
func (d *SimDevice) GoOffline(taskID string) Report {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Online = false
	return d.reportLocked(taskID, ReportOffline, "")
}

// Reconnect 模拟设备重连，返回重连宣告及补发的成功上报（若有）。
func (d *SimDevice) Reconnect(taskID string) []Report {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Online = true
	reports := []Report{d.reportLocked(taskID, ReportOnline, "")}
	if d.pendingTask == taskID && d.pendingTarget != "" {
		reports = append(reports, d.reportLocked(taskID, ReportSuccess, ""))
		d.pendingTask = ""
		d.pendingTarget = ""
	}
	return reports
}

func (d *SimDevice) reportLocked(taskID string, typ ReportType, reason string) Report {
	d.seq++
	return Report{
		DeviceID: d.ID,
		TaskID:   taskID,
		Seq:      d.seq,
		Type:     typ,
		Version:  d.Version,
		Reason:   reason,
	}
}

func (d *SimDevice) failReportLocked(taskID, reason string) Report {
	d.seq++
	return Report{
		DeviceID: d.ID,
		TaskID:   taskID,
		Seq:      d.seq,
		Type:     ReportFailure,
		Version:  d.Version,
		Reason:   reason,
	}
}

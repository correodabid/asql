// Package sysinfo collects OS and Go runtime metrics for health monitoring.
package sysinfo

import (
	"os"
	"runtime"
	"sync"
	"time"
)

var (
	startTime time.Time
	startOnce sync.Once
)

func init() {
	markStart()
}

func markStart() {
	startOnce.Do(func() {
		startTime = time.Now()
	})
}

// Snapshot is a point-in-time capture of system and runtime health.
type Snapshot struct {
	// Host
	Hostname string `json:"hostname"`
	OS       string `json:"os"`
	Arch     string `json:"arch"`
	NumCPU   int    `json:"num_cpu"`
	PID      int    `json:"pid"`

	// Go runtime
	GoVersion    string `json:"go_version"`
	NumGoroutine int    `json:"num_goroutine"`

	// Process uptime
	UptimeMS int64 `json:"uptime_ms"`

	// Memory (from runtime.MemStats)
	HeapAllocBytes  uint64  `json:"heap_alloc_bytes"`
	HeapSysBytes    uint64  `json:"heap_sys_bytes"`
	HeapInuseBytes  uint64  `json:"heap_inuse_bytes"`
	HeapObjects     uint64  `json:"heap_objects"`
	StackInuseBytes uint64  `json:"stack_inuse_bytes"`
	TotalAllocBytes uint64  `json:"total_alloc_bytes"`
	SysBytes        uint64  `json:"sys_bytes"`
	GCCycles        uint32  `json:"gc_cycles"`
	LastGCPauseNS   uint64  `json:"last_gc_pause_ns"`
	GCPauseTotalNS  uint64  `json:"gc_pause_total_ns"`
	GCCPUFraction   float64 `json:"gc_cpu_fraction"`
}

// Collect gathers a Snapshot from the current process.
func Collect() Snapshot {
	hostname, _ := os.Hostname()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	var lastPause uint64
	if mem.NumGC > 0 {
		// PauseNs is a circular buffer; most recent is at (NumGC+255)%256.
		lastPause = mem.PauseNs[(mem.NumGC+255)%256]
	}

	return Snapshot{
		Hostname:     hostname,
		OS:           runtime.GOOS,
		Arch:         runtime.GOARCH,
		NumCPU:       runtime.NumCPU(),
		PID:          os.Getpid(),
		GoVersion:    runtime.Version(),
		NumGoroutine: runtime.NumGoroutine(),
		UptimeMS:     time.Since(startTime).Milliseconds(),

		HeapAllocBytes:  mem.HeapAlloc,
		HeapSysBytes:    mem.HeapSys,
		HeapInuseBytes:  mem.HeapInuse,
		HeapObjects:     mem.HeapObjects,
		StackInuseBytes: mem.StackInuse,
		TotalAllocBytes: mem.TotalAlloc,
		SysBytes:        mem.Sys,
		GCCycles:        mem.NumGC,
		LastGCPauseNS:   lastPause,
		GCPauseTotalNS:  mem.PauseTotalNs,
		GCCPUFraction:   mem.GCCPUFraction,
	}
}

package utils

import (
		"time"
	"golang.org/x/time/rate"
	"runtime"
	"runtime/pprof"
	"github.com/golang/glog"
)

func Per(duration time.Duration, events int) rate.Limit {
	return rate.Every(duration / time.Duration(events))
}

func ConsumeMem() uint64 {
	runtime.GC()
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	return memStats.Sys
}

func DoResourceMonitor() {
	m := pprof.Lookup("goroutine")
	memStats := ConsumeMem()
	glog.V(3).Infof("Resource monitor: [%d goroutines] [%.3f kb]", m.Count(), float64(memStats) / 1e3)
}
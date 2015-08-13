package logging

import (
	"errors"
	"runtime"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
)

// The stats writer singleton.
var S StatsWriter

// The StatsWriter object.
type StatsWriter struct {
	Client      statsd.Statter // statsd package client
	isOpen      bool           // True when Open has been called and Close has not
	quit        chan struct{}  // Goroutine management
	lastGCCount uint32         // State for reporting garbage collection pauses
}

// Open allocates resources for the stats writer.
func (s *StatsWriter) Open(addr string, prefix string) error {

	var err error

	if s.isOpen {
		return errors.New("Stats Writer is already open")
	}
	s.isOpen = true

	s.Client, err = statsd.NewClient(addr, prefix)
	if err != nil {
		// Use the no-op client so we don't need to test everywhere.
		s.Client, _ = statsd.NewNoopClient()
	}

	// Report memory usage stats every second.
	s.quit = make(chan struct{})
	if err == nil {
		// Don't bother starting the ticker when using the no-op client.
		// (We still make the channel, to simplify the close.)
		statsTicker := time.NewTicker(time.Second * 1)
		go func() {
			for _ = range statsTicker.C {
				select {
				case <-s.quit:
					return
				default:
					s.sendMemoryStats()
				}
			}
		}()
	}

	return err
}

// Close releases all resources associated with the stats writer.
func (s *StatsWriter) Close() {
	if !s.isOpen {
		return
	}
	s.isOpen = false
	close(s.quit) // Stop the memory stats goroutine.
	s.Client.Close()
}

// sendMemoryStats emits the current state of memory usage to the stats sink.
func (s *StatsWriter) sendMemoryStats() {

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	s.Client.Gauge("goroutines", int64(runtime.NumGoroutine()), 1.0)
	s.Client.Gauge("memory.alloc", int64(memStats.Alloc), 1.0)
	s.Client.Gauge("memory.heap.size", int64(memStats.HeapAlloc), 1.0)
	s.Client.Gauge("memory.heap.in_use", int64(memStats.HeapInuse), 1.0)
	s.Client.Gauge("memory.heap.idle", int64(memStats.HeapIdle), 1.0)
	s.Client.Gauge("memory.heap.released", int64(memStats.HeapReleased), 1.0)
	s.Client.Gauge("memory.heap.objects", int64(memStats.HeapObjects), 1.0)
	s.Client.Gauge("memory.stack", int64(memStats.StackInuse), 1.0)
	s.Client.Gauge("memory.sys", int64(memStats.Sys), 1.0)

	// Number of GCs since the last sample
	countGC := memStats.NumGC - s.lastGCCount
	s.Client.Gauge("memory.gc", int64(countGC), 1.0)

	if countGC > 0 {
		if countGC > 256 {
			countGC = 256
		}

		for i := uint32(0); i < countGC; i++ {
			idx := ((memStats.NumGC - i) + 255) % 256
			pause := time.Duration(memStats.PauseNs[idx])
			s.Client.TimingDuration("memory.gc_pause", pause, 1.0)
		}
	}
	s.lastGCCount = memStats.NumGC
}

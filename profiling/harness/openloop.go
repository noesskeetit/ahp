package harness

import (
	"sync"
	"sync/atomic"
	"time"
)

// OpenLoopOptions describes an arrival-rate driven load: requests arrive on a
// schedule whether or not the system has finished the previous ones. A closed
// loop cannot show saturation -- when the system slows, the load generator
// slows with it, so offered load silently drops and the report calls it
// "throughput at concurrency N". Only an open loop can show queue growth and
// shedded arrivals, which is what "capacity for users" actually means.
type OpenLoopOptions struct {
	RatePerSec float64       // offered arrival rate
	Workers    int           // concurrent in-flight calls (service concurrency)
	Duration   time.Duration // measurement window
	QueueBound int           // waiting room; arrivals beyond it are shedded (default Workers)
}

// OpenLoopResult keeps the offered/completed/shedded triple explicit.
type OpenLoopResult struct {
	Rec       *LatencyRecorder
	Wall      time.Duration
	Offered   int
	Completed int
	Dropped   int
	MaxQueue  int
}

// RunOpenLoop offers work at a fixed rate and reports what happened to it.
func RunOpenLoop(o OpenLoopOptions, fn func(worker int) Call) *OpenLoopResult {
	if o.QueueBound <= 0 {
		o.QueueBound = o.Workers
	}
	if o.Workers <= 0 {
		o.Workers = 1
	}
	rec := NewLatencyRecorder()
	queue := make(chan func(), o.QueueBound)
	var wg sync.WaitGroup
	var dropped, offered, completed atomic.Int64
	var maxQueue atomic.Int64
	stop := make(chan struct{})
	for w := 0; w < o.Workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				case job := <-queue:
					job()
					completed.Add(1)
				}
			}
		}(w)
	}
	start := time.Now()
	deadlineCh := make(chan struct{})
	interval := time.Duration(float64(time.Second) / o.RatePerSec)
	if interval <= 0 {
		interval = time.Microsecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	deadline := time.AfterFunc(o.Duration, func() { close(deadlineCh) })
	defer deadline.Stop()
offerLoop:
	for {
		select {
		case <-t.C:
		case <-deadlineCh:
			break offerLoop
		}
		offered.Add(1)
		worker := int(offered.Load() % int64(o.Workers))
		job := func() { rec.Record(fn(worker)) }
		select {
		case queue <- job:
			if d := len(queue); int64(d) > maxQueue.Load() {
				maxQueue.Store(int64(d))
			}
		default:
			// No waiting room left: the arrival is shedded. It is still a
			// request the system refused, so it is recorded as a failure --
			// dropping it from the statistics would hide exactly the
			// overload signal this mode exists to measure.
			dropped.Add(1)
			rec.Record(Call{Op: "shedded_arrival", ErrClass: "shedded_at_capacity"})
		}
	}
	t.Stop()
	close(stop)
	wg.Wait()
	off, dr, mq := int(offered.Load()), int(dropped.Load()), int(maxQueue.Load())
	rec.SetOffered(off, dr, mq)
	return &OpenLoopResult{
		Rec:       rec,
		Wall:      time.Since(start),
		Offered:   off,
		Completed: int(completed.Load()),
		Dropped:   dr,
		MaxQueue:  mq,
	}
}

package gokubegate

import (
	"math/rand/v2"
	"sync/atomic"
)

// Strategy picks one backend per request from the current snapshot.
// Implementations must be safe for concurrent use.
type Strategy interface {
	Pick(backends []*PodBackend) *PodBackend
}

// RoundRobin selects backends in stable order, one per request. The counter
// starts at a random offset so that multiple processes do not synchronously
// hammer the same backend.
type RoundRobin struct {
	counter atomic.Uint64
}

// NewRoundRobin creates a round-robin strategy with a random initial offset.
func NewRoundRobin() *RoundRobin {
	r := &RoundRobin{}
	r.counter.Store(rand.Uint64())
	return r
}

// Pick implements Strategy.
func (r *RoundRobin) Pick(backends []*PodBackend) *PodBackend {
	n := uint64(len(backends))
	if n == 0 {
		return nil
	}
	return backends[r.counter.Add(1)%n]
}

// Random selects a backend uniformly at random.
type Random struct{}

// Pick implements Strategy.
func (Random) Pick(backends []*PodBackend) *PodBackend {
	if len(backends) == 0 {
		return nil
	}
	return backends[rand.IntN(len(backends))]
}

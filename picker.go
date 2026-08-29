package gokubegate

import "math/rand/v2"

// Strategy picks one backend per request from the current snapshot.
// Implementations must be safe for concurrent use.
type Strategy interface {
	Pick(backends []*PodBackend) *PodBackend
}

// RoundRobin selects backends in stable order, one per request. The counter
// starts at a random offset so that multiple processes do not synchronously
// hammer the same backend.
type RoundRobin struct {
	counter uint64
}

// NewRoundRobin creates a round-robin strategy with a random initial offset.
func NewRoundRobin() *RoundRobin {
	return &RoundRobin{counter: rand.Uint64()}
}

// Pick implements Strategy.
func (r *RoundRobin) Pick(backends []*PodBackend) *PodBackend {
	n := uint64(len(backends))
	if n == 0 {
		return nil
	}
	r.counter++
	return backends[r.counter%n]
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

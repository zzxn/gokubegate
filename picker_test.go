package gokubegate

import (
	"fmt"
	"testing"
)

func mkBackends(n int) []*PodBackend {
	out := make([]*PodBackend, n)
	for i := 0; i < n; i++ {
		out[i] = &PodBackend{
			key:     EndpointKey{Address: fmt.Sprintf("10.0.0.%d", i+1), Port: 80},
			address: fmt.Sprintf("10.0.0.%d:80", i+1),
			label:   fmt.Sprintf("pod-%d", i),
		}
	}
	return out
}

func TestRoundRobinIsBalanced(t *testing.T) {
	rr := NewRoundRobin()
	backends := mkBackends(3)

	counts := map[string]int{}
	for i := 0; i < 300; i++ {
		b := rr.Pick(backends)
		counts[b.label]++
	}
	for _, c := range counts {
		if c != 100 {
			t.Fatalf("round robin not balanced over full cycles: %v", counts)
		}
	}
}

func TestRoundRobinCyclesThroughAll(t *testing.T) {
	rr := NewRoundRobin()
	backends := mkBackends(4)

	seen := map[string]bool{}
	for i := 0; i < 4; i++ {
		seen[rr.Pick(backends).label] = true
	}
	if len(seen) != 4 {
		t.Fatalf("expected all backends picked in one cycle, got %v", seen)
	}
}

func TestRoundRobinEmpty(t *testing.T) {
	rr := NewRoundRobin()
	if b := rr.Pick(nil); b != nil {
		t.Fatalf("expected nil for empty backend list, got %v", b)
	}
}

func TestRandomPicksFromSet(t *testing.T) {
	backends := mkBackends(3)
	seen := map[string]int{}
	for i := 0; i < 300; i++ {
		b := (Random{}).Pick(backends)
		seen[b.label]++
	}
	if len(seen) != 3 {
		t.Fatalf("expected all backends reachable, got %v", seen)
	}
}

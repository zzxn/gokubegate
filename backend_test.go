package gokubegate

import (
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestInflightBodyCloseIsIdempotent(t *testing.T) {
	var n atomic.Int64
	b := &inflightBody{
		ReadCloser: io.NopCloser(strings.NewReader("x")),
		onClose:    func() { n.Add(1) },
	}
	if err := b.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if n.Load() != 1 {
		t.Fatalf("onClose invoked %d times, want 1", n.Load())
	}
}

func TestDrainWaitsForInflight(t *testing.T) {
	cfg := defaultConfig()
	cfg.Service = "svc"
	cfg.DrainTimeout = 3 * time.Second

	b := newPodBackend(
		EndpointKey{Namespace: "ns", Service: "svc", Address: "10.0.0.1", Port: 80},
		"10.0.0.1:80", "pod-1", "", "svc.ns.svc.cluster.local:80", cfg,
	)
	if !b.tryAcquire() {
		t.Fatal("initial request should be admitted")
	}

	done := make(chan struct{})
	b.startDrain(cfg, func() { close(done) })

	select {
	case <-done:
		t.Fatal("drain completed while inflight > 0")
	case <-time.After(100 * time.Millisecond):
	}

	b.release()

	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("drain did not complete after inflight cleared")
	}
	if !b.draining.Load() {
		t.Fatal("backend should be marked draining")
	}
}

func TestDrainSynchronouslyRejectsNewAdmissions(t *testing.T) {
	cfg := defaultConfig()
	cfg.Service = "svc"
	cfg.DrainTimeout = time.Second

	b := newPodBackend(
		EndpointKey{Namespace: "ns", Service: "svc", Address: "10.0.0.1", Port: 80},
		"10.0.0.1:80", "pod-1", "", "svc.ns.svc.cluster.local:80", cfg,
	)
	if !b.tryAcquire() {
		t.Fatal("initial request should be admitted")
	}

	done := make(chan struct{})
	b.startDrain(cfg, func() { close(done) })
	if b.tryAcquire() {
		t.Fatal("request admitted after draining began")
	}
	if got := b.inflight.Load(); got != 1 {
		t.Fatalf("inflight=%d, want 1", got)
	}

	b.release()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not complete after admitted request released")
	}
}

func TestDrainTimesOutWithInflight(t *testing.T) {
	cfg := defaultConfig()
	cfg.Service = "svc"
	cfg.DrainTimeout = 50 * time.Millisecond

	b := newPodBackend(
		EndpointKey{Namespace: "ns", Service: "svc", Address: "10.0.0.1", Port: 80},
		"10.0.0.1:80", "pod-1", "", "svc.ns.svc.cluster.local:80", cfg,
	)
	b.inflight.Add(5)

	done := make(chan struct{})
	b.startDrain(cfg, func() { close(done) })

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("drain should have timed out")
	}
	if b.inflight.Load() != 5 {
		t.Fatalf("inflight should be untouched, got %d", b.inflight.Load())
	}
}

func TestEndpointLabel(t *testing.T) {
	if got := endpointLabel("pod-1", "10.0.0.1:80"); got != "pod-1" {
		t.Fatalf("pod name should be used, got %q", got)
	}
	if got := endpointLabel("", "10.0.0.1:80"); len(got) != 12 {
		t.Fatalf("hash label should be 12 hex chars, got %q (%d)", got, len(got))
	}
}

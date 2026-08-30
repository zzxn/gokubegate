package gokubegate

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
)

func clusterIPGateForServer(t *testing.T, server *httptest.Server, hook Hook) *Gate {
	t.Helper()
	cfg := defaultConfig()
	cfg.Namespace = testNS
	cfg.Service = testService
	cfg.Mode = ModeClusterIP
	cfg.Port = 8080
	cfg.Hooks = []Hook{hook}
	cfg.resolveLogging()

	transport := newHTTPTransport(cfg, logicalServiceHost(cfg), cfg.MaxIdleConnsPerPod)
	serverAddress := server.Listener.Addr().String()
	transport.DialContext = (&net.Dialer{}).DialContext
	originalDial := transport.DialContext
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return originalDial(ctx, network, serverAddress)
	}
	gate := &Gate{
		cfg:                cfg,
		clusterIPTransport: transport,
		shouldClose:        shouldCloseClusterIPConnection,
	}
	t.Cleanup(func() { _ = gate.Close() })
	return gate
}

func TestClusterIPModeRotatesWithoutMutatingRequest(t *testing.T) {
	var sawClose atomic.Bool
	var sawHost atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawClose.Store(r.Close)
		sawHost.Store(r.Host)
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()

	var rotations atomic.Int64
	gate := clusterIPGateForServer(t, server, HookFunc(func(event Event) {
		if event.Kind == EventConnectionRotated {
			rotations.Add(1)
		}
	}))
	gate.shouldClose = func(uint64) bool { return true }

	req, err := http.NewRequest(http.MethodGet, "http://ignored.example/api/items?page=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := (&http.Client{Transport: gate}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	if !sawClose.Load() {
		t.Fatal("sampled ClusterIP request did not carry Connection: close")
	}
	if got, want := sawHost.Load(), "svc.ns.svc.cluster.local:8080"; got != want {
		t.Fatalf("downstream Host=%q, want %q", got, want)
	}
	if rotations.Load() != 1 {
		t.Fatalf("rotation events=%d, want 1", rotations.Load())
	}
	if req.Close || req.URL.Host != "ignored.example" {
		t.Fatalf("original request mutated: close=%t host=%q", req.Close, req.URL.Host)
	}
}

func TestClusterIPModeDoesNotRotateSSE(t *testing.T) {
	var sawClose atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawClose.Store(r.Close)
		_, _ = io.WriteString(w, "data: ok\n\n")
	}))
	defer server.Close()

	var rotations atomic.Int64
	gate := clusterIPGateForServer(t, server, HookFunc(func(event Event) {
		if event.Kind == EventConnectionRotated {
			rotations.Add(1)
		}
	}))
	gate.shouldClose = func(uint64) bool { return true }

	req, _ := http.NewRequest(http.MethodGet, "http://ignored.example/stream", nil)
	req.Header.Set("Accept", "text/event-stream; charset=utf-8")
	resp, err := (&http.Client{Transport: gate}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if sawClose.Load() || rotations.Load() != 0 {
		t.Fatalf("SSE request rotated: close=%t events=%d", sawClose.Load(), rotations.Load())
	}
}

func TestClusterIPModePreservesExplicitHost(t *testing.T) {
	var sawHost atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawHost.Store(r.Host)
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()
	gate := clusterIPGateForServer(t, server, nil)
	gate.shouldClose = func(uint64) bool { return false }

	req, _ := http.NewRequest(http.MethodGet, "http://ignored.example/path", nil)
	req.Host = "custom.example.com"
	resp, err := (&http.Client{Transport: gate}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if got := sawHost.Load(); got != "custom.example.com" {
		t.Fatalf("explicit Host=%q, want custom.example.com", got)
	}
}

func TestClusterIPModeResolvesServicePortWithoutEndpointSlice(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gate, err := NewGate(ctx, testNS, testService,
		WithMode(ModeClusterIP),
		WithClientset(fake.NewSimpleClientset(testServiceObject())),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Close()
	if gate.cfg.Port != 80 {
		t.Fatalf("ClusterIP port=%d, want Service port 80", gate.cfg.Port)
	}
	if endpoints := gate.Endpoints(); endpoints != nil {
		t.Fatalf("ClusterIP mode should not expose discovered endpoints: %v", endpoints)
	}
}

func TestClusterIPModeWithExplicitPortNeedsNoKubeConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := NewClient(ctx, "ns", "svc", WithMode(ModeClusterIP), WithPort(8080))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if got := client.Mode(); got != "clusterip" {
		t.Fatalf("mode=%q, want clusterip", got)
	}
}

func TestClusterIPModeValidation(t *testing.T) {
	if _, err := buildConfig("ns", "svc", WithMode("other")); err == nil {
		t.Fatal("expected unsupported mode error")
	}
	if _, err := buildConfig("ns", "svc", WithMode(ModeClusterIP), WithConnectionCloseSampleDenominator(99)); err == nil {
		t.Fatal("expected unsafe denominator error")
	}
	if _, err := buildConfig("ns", "svc", WithMode(ModeClusterIP), WithConnectionCloseSampleDenominator(0)); err != nil {
		t.Fatalf("zero denominator should disable rotation: %v", err)
	}
	cfg, err := buildConfig("ns", "svc", WithMode(ModeClusterIP))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnectionCloseSampleDenominator != 1000 {
		t.Fatalf("default denominator=%d, want 1000", cfg.ConnectionCloseSampleDenominator)
	}
}

func TestClusterIPConnectionCloseSamplerBoundaries(t *testing.T) {
	if shouldCloseClusterIPConnection(0) {
		t.Fatal("disabled sampler returned true")
	}
	for range 10 {
		if !shouldCloseClusterIPConnection(1) {
			t.Fatal("denominator 1 must always sample")
		}
	}
}

package gokubegate

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
	"time"
)

// Gate is an http.RoundTripper that load-balances each request across the
// ready pods of a Kubernetes Service, using per-pod connection pools.
// It is safe for concurrent use by multiple goroutines and can be shared by
// several http.Client instances with different timeouts.
type Gate struct {
	discovery *discovery
	strategy  Strategy
	cfg       *Config
	closed    atomic.Bool
}

// NewGate creates a Gate for the given Kubernetes Service. It blocks until
// the EndpointSlice informer cache has synced (or CacheSyncTimeout elapses).
func NewGate(ctx context.Context, namespace, service string, opts ...Option) (*Gate, error) {
	cfg, err := buildConfig(namespace, service, opts...)
	if err != nil {
		return nil, err
	}
	d, err := newDiscovery(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Gate{discovery: d, strategy: cfg.Strategy, cfg: cfg}, nil
}

// buildConfig applies options, resolves logging, and validates.
func buildConfig(namespace, service string, opts ...Option) (*Config, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}
	cfg.Namespace = namespace
	cfg.Service = service
	cfg.resolveLogging()
	if cfg.Strategy == nil {
		cfg.Strategy = NewRoundRobin()
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// RoundTrip implements http.RoundTripper. It rewrites the request to dial a
// chosen pod directly while keeping the logical service host in the Host
// header. The original request is never mutated.
func (g *Gate) RoundTrip(req *http.Request) (*http.Response, error) {
	if g.closed.Load() {
		return nil, fmt.Errorf("gokubegate: gate is closed")
	}

	start := time.Now()
	backend, err := g.pickBackend()
	if err != nil {
		return nil, err
	}
	emit(g.cfg, Event{Kind: EventEndpointPicked, Endpoint: backend.label})

	// Capture connection reuse via httptrace. GotConn fires synchronously
	// inside client.Do, before RoundTrip returns, so the value is safe to
	// read afterwards (even for streaming bodies).
	var reused bool
	traceCtx := httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused },
	})

	clone := req.Clone(traceCtx)
	rewriteRequest(g.cfg, g.discovery.logicalHost(), clone, backend)

	resp, err := backend.roundTrip(clone)
	if err != nil {
		emit(g.cfg, Event{Kind: EventRequestDone, Endpoint: backend.label, Result: "error", Duration: time.Since(start)})
		return nil, err
	}
	emit(g.cfg, Event{Kind: EventRequestDone, Endpoint: backend.label, Result: "success", Reused: reused, Duration: time.Since(start)})
	return resp, nil
}

// pickBackend reads the current snapshot and selects a non-draining backend.
// If the first pick races a removal, the snapshot is re-read once.
func (g *Gate) pickBackend() (*PodBackend, error) {
	snap := g.discovery.currentSnapshot()
	if snap.isEmpty() {
		return nil, ErrNoEndpoints
	}
	backend := g.strategy.Pick(snap.Backends)
	if backend != nil && !backend.draining.Load() {
		return backend, nil
	}
	// Re-read once; a reconcile may have just removed the picked backend.
	snap = g.discovery.currentSnapshot()
	if snap.isEmpty() {
		return nil, ErrNoEndpoints
	}
	backend = g.strategy.Pick(snap.Backends)
	if backend == nil || backend.draining.Load() {
		return nil, ErrNoEndpoints
	}
	return backend, nil
}

// rewriteRequest clones the request, points the URL at the pod address, and
// preserves the logical service authority in the Host header.
func rewriteRequest(cfg *Config, logicalHost string, clone *http.Request, backend *PodBackend) {
	u := *clone.URL
	clone.URL = &u
	if clone.URL.Scheme == "" {
		clone.URL.Scheme = cfg.Scheme
	}
	clone.URL.Host = backend.address
	if clone.Host == "" {
		clone.Host = logicalHost
	}
}

// Close stops discovery and drains all backends. It is idempotent.
func (g *Gate) Close() error {
	if g.closed.CompareAndSwap(false, true) {
		g.discovery.stop()
	}
	return nil
}

// Endpoints returns a read-only view of the current ready backends.
func (g *Gate) Endpoints() []EndpointInfo {
	snap := g.discovery.currentSnapshot()
	infos := make([]EndpointInfo, 0, len(snap.Backends))
	for _, b := range snap.Backends {
		infos = append(infos, EndpointInfo{
			Address:  b.address,
			PodName:  b.podName,
			NodeName: b.nodeName,
			Draining: b.draining.Load(),
			Inflight: b.inflight.Load(),
		})
	}
	return infos
}

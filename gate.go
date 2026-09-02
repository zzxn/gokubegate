package gokubegate

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
	"time"
)

// Gate is an http.RoundTripper that routes through either ready Pod endpoints
// or the Kubernetes Service ClusterIP, according to Config.Mode.
// It is safe for concurrent use by multiple goroutines and can be shared by
// several http.Client instances with different timeouts.
type Gate struct {
	discovery          *discovery
	clusterIPTransport *http.Transport
	strategy           Strategy
	cfg                *Config
	shouldClose        func(uint64) bool
	closed             atomic.Bool
}

// NewGate creates a Gate for the given Kubernetes Service. ModePod blocks
// until the EndpointSlice informer cache has synced; ModeClusterIP only
// resolves the Service port when one was not explicitly configured.
func NewGate(ctx context.Context, namespace, service string, opts ...Option) (*Gate, error) {
	cfg, err := buildConfig(namespace, service, opts...)
	if err != nil {
		return nil, err
	}
	g := &Gate{strategy: cfg.Strategy, cfg: cfg, shouldClose: shouldCloseClusterIPConnection}
	switch cfg.Mode {
	case ModePod:
		d, err := newDiscovery(ctx, cfg)
		if err != nil {
			return nil, err
		}
		g.discovery = d
	case ModeClusterIP:
		transport, err := newClusterIPTransport(ctx, cfg)
		if err != nil {
			return nil, err
		}
		g.clusterIPTransport = transport
	}
	return g, nil
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
	if g.cfg.Mode == ModeClusterIP {
		return g.roundTripClusterIP(req)
	}
	return g.roundTripPod(req)
}

func (g *Gate) roundTripPod(req *http.Request) (*http.Response, error) {
	start := time.Now()
	backend, err := g.acquireBackend()
	if err != nil {
		return nil, err
	}
	emit(g.cfg, Event{Kind: EventEndpointPicked, Endpoint: backend.label})

	// ClientTrace hooks may run concurrently, so keep the captured connection
	// reuse state safe to read after RoundTrip returns.
	var reused atomic.Bool
	traceCtx := httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) { reused.Store(info.Reused) },
	})

	tracedReq := req.Clone(traceCtx)
	rewriteRequest(g.cfg, g.discovery.logicalHost(), tracedReq, backend)

	resp, err := backend.roundTripAcquired(tracedReq)
	if err != nil {
		emit(g.cfg, Event{Kind: EventRequestDone, Endpoint: backend.label, Result: "error", Duration: time.Since(start)})
		return nil, err
	}
	emit(g.cfg, Event{Kind: EventRequestDone, Endpoint: backend.label, Result: "success", Reused: reused.Load(), Duration: time.Since(start)})
	return resp, nil
}

func (g *Gate) roundTripClusterIP(req *http.Request) (*http.Response, error) {
	start := time.Now()
	var reused atomic.Bool
	traceCtx := httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) { reused.Store(info.Reused) },
	})
	tracedReq := req.Clone(traceCtx)
	u := *tracedReq.URL
	tracedReq.URL = &u
	originalAuthority := tracedReq.URL.Host
	if tracedReq.URL.Scheme == "" {
		tracedReq.URL.Scheme = g.cfg.Scheme
	}
	logicalHost := logicalServiceHost(g.cfg)
	tracedReq.URL.Host = logicalHost
	if tracedReq.Host == "" || tracedReq.Host == originalAuthority {
		tracedReq.Host = logicalHost
	}
	if !tracedReq.Close && !isEventStreamRequest(tracedReq) && g.shouldClose(g.cfg.ConnectionCloseSampleDenominator) {
		tracedReq.Close = true
		emit(g.cfg, Event{Kind: EventConnectionRotated})
	}

	resp, err := g.clusterIPTransport.RoundTrip(tracedReq)
	if err != nil {
		emit(g.cfg, Event{Kind: EventRequestDone, Result: "error", Duration: time.Since(start)})
		return nil, err
	}
	emit(g.cfg, Event{Kind: EventRequestDone, Result: "success", Reused: reused.Load(), Duration: time.Since(start)})
	return resp, nil
}

// acquireBackend reads the current snapshot and atomically accounts for the
// request before the selected backend can begin draining. If the first pick
// races a removal, the snapshot is re-read once.
func (g *Gate) acquireBackend() (*PodBackend, error) {
	snap := g.discovery.currentSnapshot()
	if snap.isEmpty() {
		return nil, ErrNoEndpoints
	}
	backend := g.strategy.Pick(snap.Backends)
	if backend != nil && backend.tryAcquire() {
		return backend, nil
	}
	// Re-read once; a reconcile may have just removed the picked backend.
	snap = g.discovery.currentSnapshot()
	if snap.isEmpty() {
		return nil, ErrNoEndpoints
	}
	backend = g.strategy.Pick(snap.Backends)
	if backend == nil || !backend.tryAcquire() {
		return nil, ErrNoEndpoints
	}
	return backend, nil
}

// rewriteRequest points an already-cloned request at the pod address while
// preserving the logical service authority in the Host header.
func rewriteRequest(cfg *Config, logicalHost string, outReq *http.Request, backend *PodBackend) {
	u := *outReq.URL
	outReq.URL = &u
	if outReq.URL.Scheme == "" {
		outReq.URL.Scheme = cfg.Scheme
	}
	outReq.URL.Host = backend.address
	if outReq.Host == "" {
		outReq.Host = logicalHost
	}
}

// Close stops discovery and drains all backends. It is idempotent.
func (g *Gate) Close() error {
	if g.closed.CompareAndSwap(false, true) {
		if g.discovery != nil {
			g.discovery.stop()
		}
		if g.clusterIPTransport != nil {
			g.clusterIPTransport.CloseIdleConnections()
		}
	}
	return nil
}

// Endpoints returns a read-only view of the current ready backends.
func (g *Gate) Endpoints() []EndpointInfo {
	if g.discovery == nil {
		return nil
	}
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

package gokubegate

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// PodBackend owns the isolated connection pool for one pod endpoint.
// It is created by the reconciler and never shared across endpoints.
type PodBackend struct {
	key      EndpointKey
	address  string // host:port dialed
	podName  string
	nodeName string
	label    string // short stable label for events/metrics

	transport *http.Transport
	inflight  atomic.Int64
	draining  atomic.Bool
	admission sync.Mutex
	closed    chan struct{}
	closeOnce sync.Once
}

func newPodBackend(key EndpointKey, address, podName, nodeName, logicalHost string, cfg *Config) *PodBackend {
	tr := newHTTPTransport(cfg, logicalHost, cfg.MaxIdleConnsPerPod)

	return &PodBackend{
		key:       key,
		address:   address,
		podName:   podName,
		nodeName:  nodeName,
		label:     endpointLabel(podName, address),
		transport: tr,
		closed:    make(chan struct{}),
	}
}

func newHTTPTransport(cfg *Config, logicalHost string, maxIdle int) *http.Transport {
	var tlsConfig *tls.Config
	if cfg.Scheme == "https" {
		// TLS ServerName must be the logical service hostname, not the pod IP,
		// so certificates must cover the service hostname.
		serverName := logicalHost
		if host, _, err := net.SplitHostPort(logicalHost); err == nil {
			serverName = host
		}
		tlsConfig = &tls.Config{ServerName: serverName, MinVersion: tls.VersionTLS12}
	}

	return &http.Transport{
		MaxIdleConns:          maxIdle,
		MaxIdleConnsPerHost:   maxIdle,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		TLSHandshakeTimeout:   cfg.DialTimeout,
		ExpectContinueTimeout: time.Second,
		TLSClientConfig:       tlsConfig,
		DialContext: (&net.Dialer{
			Timeout:   cfg.DialTimeout,
			KeepAlive: cfg.TCPKeepAlive,
		}).DialContext,
		// HTTP/2 is intentionally disabled in v0.1: multiplexed long-lived
		// connections would defeat request-level balancing. The per-pod
		// transport boundary keeps the option open for the future.
		ForceAttemptHTTP2:     false,
		DisableCompression:    false,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
		DisableKeepAlives:     false,
	}
}

// tryAcquire atomically admits a request and accounts for it before draining
// can begin. Once draining is set, no new request can be admitted.
func (b *PodBackend) tryAcquire() bool {
	b.admission.Lock()
	defer b.admission.Unlock()
	if b.draining.Load() {
		return false
	}
	b.inflight.Add(1)
	return true
}

func (b *PodBackend) release() {
	if b.inflight.Add(-1) == 0 && b.draining.Load() {
		// A request may finish after the drain timeout. Close the connection if
		// it became idle after the drain worker's final cleanup.
		b.closeIdle()
	}
}

// roundTripAcquired performs a request already accounted for by tryAcquire.
// The returned body releases that admission exactly once when closed.
func (b *PodBackend) roundTripAcquired(req *http.Request) (*http.Response, error) {
	resp, err := b.transport.RoundTrip(req)
	if err != nil {
		b.release()
		return nil, err
	}
	resp.Body = &inflightBody{ReadCloser: resp.Body, onClose: b.release}
	return resp, nil
}

// closeIdle closes idle keep-alive connections of this backend.
func (b *PodBackend) closeIdle() { b.transport.CloseIdleConnections() }

// startDrain removes the backend from rotation and waits for in-flight
// requests to finish (or DrainTimeout). In-flight requests are never
// interrupted. onDone is called when the drain finishes.
func (b *PodBackend) startDrain(cfg *Config, onDone func()) {
	b.admission.Lock()
	b.draining.Store(true)
	b.admission.Unlock()

	go func() {
		start := time.Now()
		b.closeIdle()

		deadline := time.Now().Add(cfg.DrainTimeout)
		for b.inflight.Load() > 0 {
			if time.Now().After(deadline) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		// Close again to drop connections that became idle during draining.
		b.closeIdle()

		inflight := b.inflight.Load()
		timedOut := inflight > 0
		b.closeOnce.Do(func() { close(b.closed) })

		result := "completed"
		if timedOut {
			result = "timeout"
		}
		emit(cfg, Event{
			Kind:     EventEndpointDrained,
			Endpoint: b.label,
			Inflight: inflight,
			Result:   result,
			Duration: time.Since(start),
		})
		if onDone != nil {
			onDone()
		}
	}()
}

// inflightBody wraps a response body and invokes onClose exactly once.
type inflightBody struct {
	io.ReadCloser
	once    sync.Once
	onClose func()
}

// Close implements io.Closer.
func (b *inflightBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.onClose)
	return err
}

// endpointLabel returns a short stable label for events and metrics:
// the pod name when available, otherwise a hash of the dial address.
func endpointLabel(podName, address string) string {
	if podName != "" {
		return podName
	}
	sum := sha256.Sum256([]byte(address))
	return hex.EncodeToString(sum[:6])
}

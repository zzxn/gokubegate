package gokubegate

import (
	"context"
	"net/http"
)

// Client is an easy-to-use wrapper around a Gate. It embeds *http.Client so
// Get/Post/Do and the rest of the standard methods are available directly.
type Client struct {
	*http.Client
	gate *Gate
}

// NewClient creates a Client for the given Kubernetes Service. ModePod
// balances across ready Pods and waits for informer sync; ModeClusterIP uses
// one shared Transport through the Service address.
func NewClient(ctx context.Context, namespace, service string, opts ...Option) (*Client, error) {
	gate, err := NewGate(ctx, namespace, service, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{
		Client: &http.Client{Transport: gate},
		gate:   gate,
	}, nil
}

// Close stops discovery and drains in-flight requests. It is idempotent.
func (c *Client) Close() error { return c.gate.Close() }

// Endpoints returns a read-only view of current ready Pod backends. It returns
// nil in ModeClusterIP because that mode does not discover EndpointSlices.
func (c *Client) Endpoints() []EndpointInfo { return c.gate.Endpoints() }

// RoundTripper exposes the underlying http.RoundTripper so advanced users can
// share it across multiple http.Client instances (e.g. one with a short
// timeout and one with no timeout for SSE streams).
func (c *Client) RoundTripper() http.RoundTripper { return c.gate }

// Mode reports the configured routing mode: "pod" or "clusterip".
func (c *Client) Mode() string { return string(c.gate.cfg.Mode) }

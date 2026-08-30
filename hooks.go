package gokubegate

import "time"

// EventKind identifies the kind of a gokubegate runtime event.
type EventKind string

const (
	// EventEndpointsUpdated fires after the endpoint snapshot is reconciled.
	EventEndpointsUpdated EventKind = "endpoints_updated"
	// EventEndpointPicked fires once per request after an endpoint is selected.
	EventEndpointPicked EventKind = "endpoint_picked"
	// EventRequestDone fires when a request completes (response received or error).
	EventRequestDone EventKind = "request_done"
	// EventReconcile fires after each reconcile attempt.
	EventReconcile EventKind = "reconcile"
	// EventEndpointDrained fires when a removed endpoint finishes draining.
	EventEndpointDrained EventKind = "endpoint_drained"
	// EventConnectionRotated fires when ModeClusterIP samples an ordinary
	// request for connection close. It does not fire for explicit req.Close.
	EventConnectionRotated EventKind = "connection_rotated"
)

// Event is delivered to all registered hooks. Fields are only meaningful for
// specific kinds; hooks should switch on Kind. Endpoint is empty for
// ModeClusterIP request events because kube-proxy owns endpoint selection.
type Event struct {
	Kind    EventKind
	Service string // target service name
	// Endpoint is a short stable endpoint label (pod name or address hash).
	// Empty when not applicable.
	Endpoint string

	// Ready and Draining are set for EventEndpointsUpdated.
	Ready    int
	Draining int
	// Result values: EventRequestDone -> "success"|"error";
	// EventReconcile -> "success"|"error"; EventEndpointDrained -> "completed"|"timeout".
	Result string
	// Reused is set for EventRequestDone: whether the outbound connection was reused.
	Reused bool
	// Err is set for EventReconcile when Result == "error".
	Err error
	// Duration is set for EventRequestDone: full round-trip duration.
	Duration time.Duration
}

// Hook receives every gokubegate runtime event. Implementations must return
// quickly and must not block, retry, or re-enter gokubegate; events are
// delivered synchronously on the request/reconcile path.
type Hook interface {
	Handle(Event)
}

// HookFunc adapts a function to the Hook interface.
type HookFunc func(Event)

// Handle implements Hook.
func (f HookFunc) Handle(e Event) { f(e) }

// emit delivers e to all registered hooks in registration order.
func emit(cfg *Config, e Event) {
	if len(cfg.Hooks) == 0 {
		return
	}
	e.Service = cfg.Service
	for _, h := range cfg.Hooks {
		if h != nil {
			h.Handle(e)
		}
	}
}

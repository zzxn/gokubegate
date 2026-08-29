package gokubegate

import (
	"fmt"
	"net"
	"strconv"
	"time"
)

// EndpointKey uniquely identifies a pod backend. It includes the pod UID so
// that a new pod reusing the same IP gets a fresh backend and connection pool.
type EndpointKey struct {
	Namespace string
	Service   string
	UID       string // targetRef.uid; may be empty
	Address   string // IP (IPv4 or IPv6)
	Port      int32
}

// String returns a stable, sortable representation of the key.
func (k EndpointKey) String() string {
	return fmt.Sprintf("%s/%s/%s/%s/%d", k.Namespace, k.Service, k.UID, k.Address, k.Port)
}

// EndpointInfo is a read-only view of a backend for debugging/observability.
type EndpointInfo struct {
	Address  string // host:port actually dialed
	PodName  string
	NodeName string
	Draining bool
	Inflight int64
}

// EndpointSnapshot is an immutable set of backends. It is published atomically
// and never mutated after publication.
type EndpointSnapshot struct {
	Version  uint64
	Backends []*PodBackend
	Updated  time.Time
}

func (s *EndpointSnapshot) isEmpty() bool {
	return s == nil || len(s.Backends) == 0
}

// addressHostPort builds the dial address for an endpoint.
func addressHostPort(addr string, port int32) string {
	return net.JoinHostPort(addr, strconv.Itoa(int(port)))
}

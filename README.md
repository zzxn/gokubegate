# gokubegate

Client-side pod load balancing for Go services running inside Kubernetes.

`gokubegate` is a generic Go client library with two routing modes: deterministic
request-level balancing across ready EndpointSlice Pods, or a shared Transport
through the Service ClusterIP with sampled connection rotation.

It is a generalization of the client-side pod load-balancing design validated
in a production Kubernetes gateway (see design rationale in
[docs/spec/technical-design.md](./docs/spec/technical-design.md)).

## Features

- `gokubegate.NewClient(namespace, service, ...)` — ready-to-use `*http.Client`
- Automatic `EndpointSlice` discovery and pod readiness/terminating filtering
- Round-robin (default) / random request-level load balancing
- Isolated per-pod connection pools with graceful drain on scale-down
- Optional `clusterip` mode with one shared Transport and sampled connection
  rotation for environments where direct Pod routing is not desired
- Last-known-good snapshot when the Kubernetes API server is briefly unavailable
- Zero metrics/logging dependencies — observability via event hooks
  (`WithHook`), logging via `WithLogger` / `WithDebug`

## Quick start

```go
package main

import (
    "context"
    "net/http"

    "github.com/zzxn/gokubegate"
)

func main() {
    ctx := context.Background()

    client, err := gokubegate.NewClient(ctx, "demo", "demo-chat")
    if err != nil {
        panic(err) // config / RBAC / cache sync errors surface here
    }
    defer client.Close()

    req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
        "http://demo-chat.demo.svc.cluster.local:8888/demo/v2/chat/status", nil)
    resp, err := client.Do(req) // routed to one of the ready pods
    if err != nil {
        panic(err)
    }
    defer resp.Body.Close()
}
```

Required RBAC for `pod` mode (in-cluster):

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: gokubegate-endpointslice-reader
  namespace: demo
rules:
  - apiGroups: ["discovery.k8s.io"]
    resources: ["endpointslices"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["services"]   # only when the port is auto-resolved
    verbs: ["get"]
```

## ClusterIP mode

`clusterip` keeps Kubernetes Service routing and uses a single shared
`http.Transport`. By default, approximately 0.1% of ordinary HTTP requests close
their connection after the response, encouraging kube-proxy to choose another
endpoint for later connections:

```go
client, err := gokubegate.NewClient(ctx, "demo", "demo-chat",
    gokubegate.WithMode(gokubegate.ModeClusterIP),
    gokubegate.WithConnectionCloseSampleDenominator(1000),
)
```

Use denominator `0` to disable rotation. Non-zero values must be at least 100.
SSE requests advertising `Accept: text/event-stream` are never sampled. This
mode improves probabilistic distribution but does not provide the exact
request-level balancing of `pod` mode.

When `WithPort` is supplied, `clusterip` mode needs no Kubernetes API access.
Otherwise it only needs `get` permission for the target Service to resolve its
Service port; it does not watch EndpointSlices.

## Capacity guidance

Measured against four downstream Pods limited to 1 CPU / 1 GiB serving simple
sub-10 ms responses (see the e2e report):

- `pod` mode sustains **5,000 QPS per Pod** with zero errors and exact per-Pod
  balance at every tested idle budget (8/16/32/64/128/512). Saturation
  throughput grows 33k → 70k req/s across four Pods as the idle budget rises,
  flattening after 64.
- `clusterip` mode tops out at roughly **2,700 QPS per Pod** (~11k req/s across
  four Pods) and cannot meet the same target; it is only for environments where
  direct Pod routing is not allowed.
- Default `MaxIdleConnsPerPod=32` sits at the knee of the curve (16→32 adds
  +31% saturation, 32→64 only +12%) with the lowest p99 at target load (4.6 ms
  at 5,000 QPS/Pod). idle8 barely passes (p99 9.95 ms, 95% reuse); 64 buys more
  headroom (68k req/s); 128/512 add nothing further. Budget idle connections as
  downstream Pods × idle per Pod × replicas.

## Documentation

- [docs/spec/technical-design.md](./docs/spec/technical-design.md) — full
  technical design (API, architecture, lifecycle, failure handling, testing)
- [docs/spec/e2e-integration-test-plan.md](./docs/spec/e2e-integration-test-plan.md) —
  real-cluster integration test plan (kind + Docker)
- [docs/spec/benchmark-methodology.md](./docs/spec/benchmark-methodology.md) —
  how the mode/parameter benchmark is set up and which metrics it records
- [test/e2e/README.md](./test/e2e/README.md) — running the real-cluster suite

## Requirements

- Go 1.26+
- Kubernetes cluster (client-go v0.37)

## License

Apache-2.0. See [LICENSE](./LICENSE).

# gokubegate

Client-side pod load balancing for Go services running inside Kubernetes.

`gokubegate` is a generic Go client library that watches a target Kubernetes
Service's `EndpointSlice`, and load-balances each HTTP request across the
currently Ready Pods at the **request level** — instead of relying on the
Service ClusterIP's TCP-connection-level balancing, which pins keep-alive
connections to a few Pods.

It is a generalization of the client-side pod load-balancing design validated
in a production Kubernetes gateway (see design rationale in
[docs/spec/technical-design.md](./docs/spec/technical-design.md)).

## Features

- `gokubegate.NewClient(namespace, service, ...)` — ready-to-use `*http.Client`
- Automatic `EndpointSlice` discovery and pod readiness/terminating filtering
- Round-robin (default) / random request-level load balancing
- Isolated per-pod connection pools with graceful drain on scale-down
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

Required RBAC (in-cluster):

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

## Documentation

- [docs/spec/technical-design.md](./docs/spec/technical-design.md) — full
  technical design (API, architecture, lifecycle, failure handling, testing)
- [docs/spec/e2e-integration-test-plan.md](./docs/spec/e2e-integration-test-plan.md) —
  real-cluster integration test plan (kind + Docker)

## Requirements

- Go 1.26+
- Kubernetes cluster (client-go v0.37)

## License

Apache-2.0. See [LICENSE](./LICENSE).

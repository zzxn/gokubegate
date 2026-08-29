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

## Quick start

```go
client, err := gokubegate.NewClient(ctx, "demo", "demo-chat")
if err != nil {
    panic(err)
}
defer client.Close()

resp, err := client.Get(ctx, "http://demo-chat.demo.svc.cluster.local:8888/demo/v2/chat/status")
```

See [docs/spec/technical-design.md](./docs/spec/technical-design.md) for the
full technical design.

## Status

Design draft (v0.1). Not yet implemented.

## License

TBD (Apache-2.0 planned).

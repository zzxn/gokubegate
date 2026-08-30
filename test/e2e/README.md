# End-to-end tests

These tests create a real Kubernetes cluster with kind, build two local images,
and exercise gokubegate from an in-cluster Job. The suite currently covers:

- request-level distribution and connection reuse (S1);
- logical/custom Host plus path and query preservation (S2);
- scale-up and scale-down EndpointSlice propagation (S3/S4);
- NotReady removal and readiness restoration (S4);
- successful minimal RBAC and fail-fast behavior without permissions (S5).
- SSE stream affinity across concurrent long-lived streams (S6);
- last-known-good routing while the kind control plane is paused (S7);
- 1/8/32/64 concurrency gradients and sustained 64-way rapid scaling.
- `pod` versus `clusterip` mode distribution, including ClusterIP with sampled
  rotation disabled and enabled.
- rapid scaling (`2 -> 8 -> 2 -> 6 -> 1 -> 4`) under sustained 64-way load for
  both `pod` and `clusterip` modes, with per-second pod distribution windows to
  quantify scale-up convergence.
- zero-downtime rollout replacement (10 old Pods -> 10 new Pods -> old removed)
  under sustained 128-way load for both `pod` and `clusterip` modes
  (`TestE2ERolloutReplacement` / `TestE2ERolloutReplacementClusterIP`).

The rapid-scaling scenario keeps one client active for 24 seconds while the
Deployment receives `2 -> 8 -> 2 -> 6 -> 1 -> 4` replica updates every two
seconds. It records per-second request/error/latency windows so a short outage
cannot be hidden by aggregate success rates.

## Run

Requirements: Docker, Go 1.26+, and `kubectl`. The bootstrap target installs a
pinned kind binary under `.cache/`; it does not modify system paths.

```bash
make e2e
```

Keep the default test cluster for inspection:

```bash
GOKUBEGATE_E2E_KEEP=1 make e2e
```

Reuse an explicitly named kind cluster (the suite will preserve it):

```bash
GOKUBEGATE_E2E_CLUSTER=my-kind make e2e
```

Run one scenario or clean local test artifacts:

```bash
make e2e ARGS='-run TestE2ENotReady'
make e2e-clean
```

On failure, rerun with `GOKUBEGATE_E2E_KEEP=1`, then inspect resources with the
isolated kubeconfig at `.cache/e2e/kubeconfig`:

```bash
kubectl --kubeconfig .cache/e2e/kubeconfig -n gokubegate-e2e get pods,endpointslices
```

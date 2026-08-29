# End-to-end tests

These tests create a real Kubernetes cluster with kind, build two local images,
and exercise gokubegate from an in-cluster Job. The suite currently covers:

- request-level distribution and connection reuse (S1);
- logical/custom Host plus path and query preservation (S2);
- scale-up and scale-down EndpointSlice propagation (S3/S4);
- NotReady removal and readiness restoration (S4);
- successful minimal RBAC and fail-fast behavior without permissions (S5).

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

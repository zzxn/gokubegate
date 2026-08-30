> [简体中文](./e2e-integration-test-plan.zh.md)

# gokubegate Real-Cluster Integration Test Implementation Plan

## Document status

- Status: Plan
- Date: 2026-08-29
- Scope: `test/e2e` real-Kubernetes-cluster integration tests (based on kind + Docker)
- This is an implementation plan; it corresponds to section 13.2 (integration testing) of the design document [technical-design.md](./technical-design.md)

## 1. Background and goals

Unit tests use a fake clientset to verify internal logic such as the informer/aggregation/Picker, but they cannot cover:

- EndpointSlices produced by a real kube-apiserver + controller-manager (controller semantics, condition fields, deletion timing);
- Real Pod networking, Service/endpoint scaling, and readiness changes;
- Real in-cluster config, ServiceAccount, and RBAC behavior at runtime.

This plan uses **kind** (Kubernetes in Docker) to start a real K8s cluster inside Docker, containerizes all test components (downstream service, load client), and deletes the whole cluster when the test finishes, **without polluting the host environment** (no changes to existing cluster config, no host ports, no lingering services).

### Acceptance targets (scenario overview)

| ID | Scenario | One-line acceptance criterion |
| --- | --- | --- |
| S1 | Basic load balancing | 2-replica downstream, 500 requests distributed per-request, per-Pod hits within tolerance |
| S2 | Host/path fidelity | downstream receives the logical Service domain as Host; path/query preserved verbatim |
| S3 | Scale-up | 2→4 replicas; new Pods start receiving traffic after Ready |
| S4 | Scale-down / NotReady | removed/NotReady Pods stop receiving new requests |
| S5 | RBAC | minimal permissions work; missing permissions fail `NewClient` at startup |
| S6 | SSE long connections (optional) | a stream stays on one Pod; new streams keep balancing |
| S7 | API server disruption (optional) | last-known-good forwarding continues while the informer is disconnected |

## 2. Non-goals (not this phase)

- Performance/capacity load testing (separate benchmark; see design document 13.3);
- mTLS/sidecar path verification under a service mesh (Istio/Linkerd);
- Multi-cluster, cross-region;
- HTTP/2 verification (not enabled in v0.1);
- Platforms other than Windows/macOS (the host is Linux; Docker Desktop can run kind).

## 3. Technology choices

### 3.1 Why kind

- **Real cluster**: kind starts a real kube-apiserver, kube-controller-manager, kubelet, and CNI (kindnet); EndpointSlices are produced by the real controller, with production-equivalent semantics;
- **Fully containerized**: control plane and nodes are Docker containers; `kind delete cluster` cleans everything up;
- **Officially maintained**: produced by kubernetes-sigs, CI-friendly, with an official GitHub Action;
- **Preloadable images**: `kind load docker-image` loads locally built images directly, no registry needed.

Alternative: k3d (k3s in Docker) is lighter and faster, but k3s uses its own Service proxy and trimmed components, so EndpointSlice/readiness behavior differs from standard K8s. Recorded as an alternative; kind is the default.

### 3.2 Why not envtest

envtest only starts kube-apiserver + etcd binaries, with no kubelet/nodes/Pods/network, so it cannot verify real Pod and EndpointSlice lifecycles and does not satisfy the "real cluster" requirement.

### 3.3 Docker isolation strategy

- kind cluster: all components run in Docker containers, no host ports (kind allocates them), no host kubeconfig changes (test kubeconfig lives under `.cache/e2e/`);
- Test images: `downstream` and `tester` are built locally + `kind load`, no registry push needed;
- Cleanup: `t.Cleanup` / `make e2e-clean` remove the namespace, Job, and Deployment; `kind delete cluster` removes the cluster; `GOKUBEGATE_E2E_KEEP=1` keeps the cluster for debugging;
- Full-isolation mode (optional enhancement): `make e2e-dind` runs the whole suite inside a Docker container (mounting docker.sock), so the host only needs Docker — not even kind/kubectl/go.

### 3.4 Image builds

- `downstream`: multi-stage, built with `golang:1.26`, run on `alpine`/`distroless`;
- `tester`: also multi-stage, imports `github.com/zzxn/gokubegate`, runs as a Job;
- Build cache: leverage `docker build` layer caching; image tags use short commit SHAs to avoid staleness.

## 4. Environment requirements

| Dependency | Version/notes | Current host |
| --- | --- | --- |
| Docker | >= 24 | ✅ 29.1.2 (Docker Desktop) |
| kind CLI | v0.2x (a version will be pinned) | ❌ not installed → bootstrap script downloads to `.cache/kind/` |
| kubectl | optional (the driver can use client-go) | ✅ present |
| Go | 1.26+ (driver and image builds) | ✅ 1.26.1 |
| Memory | kind single cluster suggests >= 4GB | ✅ 30GB |

Bootstrap script `scripts/e2e-bootstrap.sh`:

```bash
# download kind to .cache/kind/ (not into the system), verify sha256
KIND_VERSION=${KIND_VERSION:-v0.27.0}
curl -Lo .cache/kind/kind "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-amd64"
chmod +x .cache/kind/kind
```

## 5. Directory structure and components

```text
test/e2e/
  driver_test.go        // main entry: //go:build e2e gate; kind lifecycle + scenario orchestration + assertions
  harness/
    kind.go             // create/delete/reuse kind cluster, fetch temporary kubeconfig
    kube.go             // client-go wrappers: apply manifests, wait Ready, scale, read logs
    wait.go             // polling wait (configurable timeout)
  downstream/
    main.go             // downstream HTTP service: returns Pod name/IP; /ready toggles readiness; /stats counts
    Dockerfile
  tester/
    main.go             // load CLI using gokubegate: sends N requests, outputs JSON results
    Dockerfile
  manifests/
    namespace.yaml      // gokubegate-e2e
    rbac.yaml           // ServiceAccount + Role(endpointslices rw + services get) + RoleBinding
    rbac-denied.yaml    // a copy with only the wrong Role (for S5)
    downstream.yaml     // Deployment(replica count overridable) + Service + readinessProbe
    tester.yaml         // Job template (parameterized command: request count / target URL / output)
  README.md             // how to run, FAQ
Makefile                // make e2e / e2e-e2e-down / e2e-clean / e2e-dind
scripts/
  e2e-bootstrap.sh      // download kind to .cache
  e2e-dind.sh           // optional: full-isolation DinD entrypoint
```

### 5.1 downstream (service under test)

- Listens on 8080 (matching the Service/EndpointSlice port);
- `GET /`: returns `{pod, node, ip, host}` (`pod` read from the `POD_NAME` env / its own hostname);
- `GET /ready`: returns the readiness state (can be toggled via `/admin/ready?on=false` for S4 NotReady simulation);
- `GET /stats`: returns the request count received by this Pod (in-memory counter, plus optional logging like `x-gokubegate-count`).

### 5.2 tester (load client)

- Creates a client with `gokubegate.NewClient(ctx, ns, svc, WithHook(...))`;
- Runs `N` requests per parameters; hooks record `endpoint_picked` / `request_done` distribution and reuse;
- Outputs JSON to stdout:
  ```json
  {"phase":"baseline","requests":500,"byEndpoint":{"pod-xxx":251,"pod-yyy":249},"reusedRatio":0.98,"hostEcho":"svc.ns.svc.cluster.local:8080","errors":0}
  ```
- Exit code: non-zero only for its own errors; it does **not** make business assertions (assertions are made by the host driver).

## 6. Test scenarios and execution flow

Global driver flow (each scenario is independent or chained, see below):

1. `TestMain` / suite setup:
   - If `GOKUBEGATE_E2E_CLUSTER` names an existing cluster, reuse it (saves time); otherwise `kind create cluster` (`--name gokubegate-e2e`, 1 control-plane + 1 worker, 2GB/node);
   - Build the `downstream`/`tester` images and `kind load` them;
   - Apply namespace/RBAC/manifests, wait for the downstream Deployment to be ready;
2. Execute each scenario;
3. `t.Cleanup`: delete the Job/Deployment/namespace; `GOKUBEGATE_E2E_KEEP=1` keeps the cluster, otherwise `kind delete cluster`.

### S1 Basic load balancing

- Precondition: downstream 2 replicas Ready;
- Execute: run the tester Job (500 requests, target `http://downstream.gokubegate-e2e.svc.cluster.local:8080/`);
- Assert (host driver reads the Job log JSON + each Pod's `/stats`):
  - All requests succeed (errors=0);
  - Both Pods' hit counts are within `[40%, 60%]` (empirical tolerance for n=500, configurable);
  - `reusedRatio >= 0.9` (the connection pool is working, not reconnecting every time);
- Judgment: each downstream Pod's `/stats` is authoritative; the tester-side hook distribution is cross-validation, allowing small deviations due to "request already in flight / connection closed".

### S2 Host/path fidelity

- Precondition: same as S1;
- Execute: tester requests carry a custom path/query, e.g. `/api/items?page=1`; downstream echoes `r.Host` and `r.URL`;
- Assert: tester outputs `hostEcho == "downstream.gokubegate-e2e.svc.cluster.local:8080"`, with path/query preserved verbatim; also run one request with `req.Host=custom.example.com` and assert the Host is respected (design document 8.1).

### S3 Scale-up

- Precondition: after S1; downstream 2 replicas;
- Steps:
  1. `kubectl scale deployment downstream --replicas=4`;
  2. Wait for 4 Pods Ready (`rollout status` / poll EndpointSlice ready count == 4);
  3. Run the tester Job (500 requests);
- Assert: all 4 Pods receive traffic (each >= 5%, i.e., new Pods indeed receive traffic); all requests succeed.

### S4 Scale-down / NotReady

- Precondition: 4 replicas Ready;
- Scenario A (scale-down): `scale --replicas=2`; wait Ready == 2; run tester; assert: removed Pods' `/stats` no longer grow (compare against pre-removal counts), surviving Pods receive all traffic;
- Scenario B (NotReady): call `/admin/ready?on=false` on a Pod, wait for its readiness probe to fail and EndpointSlice ready count == 3; run tester; assert that Pod no longer receives new requests; then restore ready and assert it resumes receiving traffic;
- Timing note: after removal/NotReady, allow a 2–5s transition window (EndpointSlice propagation + informer reconcile) before starting the load.

### S5 RBAC

- Scenario A (minimal permissions): the default tester runs with `rbac.yaml` (endpointslices rw + services get); S1 passing proves minimal permissions are sufficient;
- Scenario B (missing permissions, fail-fast): run the tester with `rbac-denied.yaml` (no endpointslices permission); assert that `NewClient` returns an error during startup (log contains RBAC/list/watch rejection) and the Job exits as failed — corresponding to design document 10.1 "startup failure fail fast".

### S6 SSE long connections (optional, P3)

- downstream adds `GET /stream`: emits one SSE event every 500ms for 10s, event content includes its own Pod name;
- tester uses `http.Client{Transport: gate, Timeout: 0}` to open 1 stream and record all received event Pod names;
- Assert: the Pod name is constant within one stream; then open 20 concurrent new streams and assert all 4 Pods are selected at least once.

### S7 API server disruption (optional/advanced)

- Idea: `docker pause` the control-plane container for 20–30s; during this the informer watch disconnects, but tester requests should all succeed (last-known-good snapshot);
- Note: pausing the whole control plane affects kubelet/scheduling, so Pods may be affected and results must be interpreted carefully; if unstable, downgrade to "only verify recovery after watch disconnection", or defer to M3 with a more controllable simulation (e.g., temporarily change the informer endpoint's network policy);
- Marked experimental, does not block P1/P2.

## 7. Data collection and assertion design

- **Authoritative count**: each downstream Pod's `/stats` (in-memory counter). The driver samples before and after each load run and takes the difference;
- **Tester side**: hook event statistics (picked/request_done/reused), output as JSON;
- **Assertion timing**: assert uniformly after the Job completes (avoid mid-run timing issues); phase transitions use the `wait` polling utility (default timeout 60s, interval 500ms);
- **Tolerance parameters**: centralized as harness constants (`distributionTolerance = 0.20`, etc.) for easy tuning;
- **Logs**: tester Job logs are fully pulled via `kubectl logs`; on failure, context is printed for debugging.

## 8. How to run and CI

### 8.1 Local run

```bash
# first time: download kind to .cache
make e2e-bootstrap

# run the full e2e (create kind → test → delete)
make e2e

# keep the cluster for debugging
GOKUBEGATE_E2E_KEEP=1 make e2e

# run a specific scenario (via go test -run)
make e2e ARGS="-run TestE2EScaleUp"

# cleanup
make e2e-clean
```

Gating: `driver_test.go` uses the `//go:build e2e` build tag, so a plain `go test ./...` does **not** run e2e; only `go test -tags e2e ./test/e2e/...` does. In CI, the `GOKUBEGATE_E2E=1` environment variable provides double protection.

### 8.2 Full-isolation mode (optional enhancement)

`make e2e-dind`: run the bootstrap script + e2e inside a Docker container (`--privileged` + docker.sock mount); the host only needs Docker:

```bash
docker run --rm --privileged \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$(pwd)":/workspace -w /workspace \
  golang:1.26 bash -c "make e2e-bootstrap && make e2e"
```

### 8.3 CI (to be wired later)

GitHub Actions uses the official `kind` action (`kindest/node` pinned version). Steps: setup-go → checkout → `go build ./...` → `make e2e`. CI image pulls can configure `GOPROXY` and a kind image mirror.

## 9. Risks and mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| kind not installed / download failure | cannot start | bootstrap script + pinned version + sha256 check; clear error on failure |
| Slow image pulls (goproxy.cn or kind base image) | timeout | set `GOPROXY=https://goproxy.cn,direct`; pin and pre-warm the kind image version |
| Timing instability (EndpointSlice propagation, probe period) | flaky assertions | uniform `wait` polling + transition window; configurable tolerance; one retry on failure |
| Docker Desktop resource limits | cluster won't start | single-node config (1 control-plane + 1 worker); document min 4GB |
| Conflict with existing local cluster/kubectl | environment pollution | everything isolated in kind and `.cache/e2e/`; don't write the default host kubeconfig location |
| Occasional flaky scenarios | CI stalls | each scenario independently re-runnable; run P1 first, then extend |

## 10. Milestones

- **P1 (skeleton + basic scenarios)**: harness (kind/kube/wait), downstream, tester, Makefile, S1/S2 passing;
- **P2 (lifecycle scenarios)**: S3 scale-up, S4 scale-down/NotReady, S5 RBAC (including fail-fast verification);
- **P3 (optional + CI)**: S6 SSE, S7 API server disruption (experimental), GitHub Actions integration, README polish.

## 11. Items to confirm before implementation

1. kind pinned version (suggest v0.27.x) + matching `kindest/node` image tag;
2. Test namespace name (suggest `gokubegate-e2e`);
3. Whether to accept the bootstrap script downloading kind into `.cache/` (not a system path);
4. Whether e2e joins regular CI (currently local-first, CI later);
5. How to simulate S7 (pause control-plane vs other), and whether to downgrade to "disconnect-and-recover only" if unstable.

## 12. References

- [kind (Kubernetes in Docker)](https://kind.sigs.k8s.io/)
- [kind official GitHub Action](https://github.com/marketplace/actions/kind-kubernetes-in-docker-action)
- [Kubernetes EndpointSlice concept](https://kubernetes.io/docs/concepts/services-networking/endpoint-slices/)
- gokubegate design document: `docs/spec/technical-design.md` (13.2 integration tests, 9 lifecycle, 10 failure handling)

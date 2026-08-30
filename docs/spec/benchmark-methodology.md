> [简体中文](./benchmark-methodology.zh.md)

# gokubegate Benchmark Methodology

Status: accompanies `TestE2EModeBenchmark` (`test/e2e/harness/e2e_test.go`) and §5 of the e2e report.
This document explains how the benchmark is set up, how each metric is measured and accepted, for reproduction and extension.

## 1. Goals

Validate and compare gokubegate modes/parameters under a typical production load. Core acceptance target:

- Each downstream Pod (1 CPU / 1 GiB, responses within 10ms) sustains **5,000 QPS** without issues;
- Concrete acceptance: zero errors, p99 ≤ 10ms, and balanced per-Pod traffic (5,000 ±30%).

## 2. Test environment

| Item | Value |
| --- | --- |
| Cluster | kind v0.27.0, 1 control-plane + 1 worker |
| Host | Linux/amd64, 24 cores / 30 GiB (shared with Docker companion containers) |
| Go | go1.26.1 |
| Client location | in-cluster Kubernetes Job (in-cluster config) |
| Downstream | 4-replica Deployment, requests/limits cpu=1, memory=1Gi |
| Downstream impl | `test/e2e/downstream`: returns Pod-identity JSON, exposes `/stats` cumulative counters |

> Note: with a single kind worker + shared host resources, **absolute capacity numbers are environment-dependent observations** used for within-round relative comparison and acceptance, not formal capacity conclusions.

## 3. Configuration matrix under test

The `steadyState` list in `TestE2EModeBenchmark` defines the configurations shared by the saturation and rate-limited phases:

| Dimension | Values |
| --- | --- |
| Routing mode | `pod` (default), `clusterip` |
| Pod idle budget `MaxIdleConnsPerPod` | 8 / 16 / 32 / 64 / 128 / 512 |
| clusterip rotation rate `1/denominator` | 0 (off) / 100 / 200 / 500 / 1000 |

The scale-up convergence phase (`convergence` list) only covers the 5 clusterip rotation rates (2→4 scale-up).

Each configuration uses a **dedicated tester Job** (a fresh process and a clean connection pool), so they don't contaminate each other. clusterip configurations pin idle=16 to isolate the rotation-rate variable.

## 4. Architecture and data flow

```text
harness (go test -tags e2e)
  ├─ create kind cluster / build+load images / apply manifests
  ├─ schedule tester Job, wait for completion, parse the result JSON
  └─ read downstream /stats via kubectl exec (authoritative server-side counters)

tester (in-cluster Job, test/e2e/tester)
  ├─ create a gokubegate Client (-gate-mode / -connection-close-denominator /
  │   -max-idle-conns / -rate / -concurrency / -duration)
  ├─ N concurrent workers send requests (HTTP, optionally rate-limited)
  └─ collect client-side metrics (request results, hook events, per-second windows) → emit one line of JSON

downstream (4 × 1c1g Deployment)
  └─ each request returns {pod, host, path, query}; /stats returns cumulative counters
```

Data flow: request → gokubegate Client (picker/transport) → downstream. The tester reads the Pod name (byPod) from the response body and connection-reuse/rotation/endpoint metrics from hook events; the harness reads server-side counters for cross-validation.

## 5. Three phases

| Phase | Parameters | Duration | Purpose |
| --- | --- | --- | --- |
| 1. Saturation | 128 concurrency, unlimited | 12s | max throughput, saturation latency, connection reuse per config |
| 2. Rate-limited acceptance | `-rate 20000`, 128 concurrency | 10s | apply exactly 5,000 QPS/Pod; accept zero errors / p99 / balance |
| 3. Scale-up convergence | 2→4 replicas, 128 concurrency | 18s | first-traffic delay and share for new Pods (clusterip) |

## 6. Metric definitions and collection

### 6.1 Client side (tester → `TesterResult` JSON)

| Metric | Field | Source |
| --- | --- | --- |
| Requests/success/errors | `requests` / `success` / `errors` | counted per request |
| Error classification | `errorKinds` / `errorSamples` | `classifyError`: connection_refused / connection_reset / no_endpoints / timeout / eof / other |
| Throughput | `throughput` | success / load duration |
| Latency | `latencyP50/P95/P99/MaxMs` | percentiles of request round-trip samples |
| Connection reuse | `reused` / `reusedRatio` | hook `EventRequestDone.Reused` |
| Total requests | `connections` | count of `EventRequestDone` |
| Rotations | `rotations` | hook `EventConnectionRotated` (clusterip) |
| Distribution | `byEndpoint` / `byPod` | `EventEndpointPicked` / response-body Pod name |
| Per-second windows | `windows[]` | per-second requests/success/errors/latencyP95/byPod; short windows are not masked by aggregation |
| Endpoint timeline | `endpointUpdates[]` | `EventEndpointsUpdated` (ready/draining) |

### 6.2 Server side (downstream `/stats`)

Read each Pod's cumulative counter before and after each phase (harness `PodStats`); difference / duration = each Pod's actual QPS. This is the authoritative count, cross-validated against the client's `byPod` (e.g., the two match exactly in the saturation phase).

### 6.3 Rate limiting (tester `-rate`)

Each worker paces independently:

```text
paceInterval = 1s × concurrency / rate
send time of request i = workerStart + i × paceInterval
```

`-rate` must be used together with `-duration`; when the actual throughput cannot reach the target (e.g., clusterip saturation), workers naturally send at full speed and `achieved` falls below the target — which is itself a signal that "this configuration cannot meet the target".

### 6.4 Scale-up convergence measurement

From `windows[].byPod`, compute the second at which each new Pod first appears in traffic (firstSeen), compare against the EndpointSlice readiness time (harness records `readyAt`) to get `delayAfterReady`; the new Pods' request share within the 18s window measures convergence sufficiency.

## 7. Acceptance criteria (test assertions)

Rate-limited acceptance phase (`rate-*` subtests):

- `pod` mode: `errors == 0` and `achieved >= target × 0.95` and `p99 <= 10ms` and each Pod's QPS ∈ [3,500, 6,500] (5,000 ±30%); any failure fails that subtest;
- `clusterip`: only records MET/NOT-MET status (exploratory configs), no hard failure;
- At least one pod configuration must meet the target, otherwise the whole test fails (`rateMet == 0`).

Saturation/convergence phases: errors are only recorded, not failed — clusterip under high concurrency occasionally shows `cannot assign requested address` (client local-address allocation failure when dialing the ClusterIP, <0.01%, suspected conntrack/environment noise).

## 8. How to run

```bash
# Full benchmark (saturation + rate-limited + convergence, ~10 min, 27 subtests)
make e2e ARGS='-run TestE2EModeBenchmark'

# Rate-limited acceptance only (skip saturation/convergence)
GOKUBEGATE_E2E_BENCH_RATE_ONLY=1 make e2e ARGS='-run TestE2EModeBenchmark'

# Keep the cluster for debugging (run `kind delete cluster --name gokubegate-e2e` afterwards)
GOKUBEGATE_E2E_KEEP=1 make e2e ARGS='-run TestE2EModeBenchmark'

# Observe progress while running (redirect test output to a file and tail)
GOKUBEGATE_E2E_KEEP=1 go test -tags e2e -timeout 30m -v \
  -run TestE2EModeBenchmark ./test/e2e/harness/... > /tmp/bench.log 2>&1
```

Each subtest's conclusion line (`bench ...` / `rate ...` / `convergence ...`) is the data source for the §5 tables in the report.

## 9. Data boundaries and notes

- kind's absolute capacity is affected by the host and companion containers; **within-round configuration comparisons are reliable, cross-round numbers have random variance** (e.g., clusterip saturation throughput fluctuates between 10k–59k req/s across rounds, related to connection-pool establishment timing).
- Individual tiers such as `idle64` may show a transient p99 spike (e.g., 9.27ms); conclusions should be based on multi-tier trends rather than single points.
- The rapid-scale-down race (transient errors from dialing an already-terminated Pod, present in both modes, within a second-level window) is not covered by this benchmark; see §3 rapid scale-up/scale-down in the e2e report.
- This benchmark only tests short HTTP requests; SSE/long streams, large responses, and resource-constrained behavior need separate design (see §10).

## 10. How to extend new tiers/configurations

1. Edit the `steadyState` (saturation/rate-limited shared) and `convergence` lists in `TestE2EModeBenchmark`, adding a `benchConfig{name, mode, denominator, maxIdle}` entry;
2. The tester needs no change (`-max-idle-conns` / `-connection-close-denominator` / `-rate` are all parameterized);
3. To adjust the target (e.g., 8,000 QPS/Pod), change `targetQPSPerPod` and `targetTotal`, and update the `-rate` argument to the new target;
4. To add a metric, add the field in the tester's `metrics` and `TesterResult`, and in the harness's `TesterResult` (JSON mirror) in lockstep.

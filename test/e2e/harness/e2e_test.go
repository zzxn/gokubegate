//go:build e2e

package harness

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

var h *Harness

func TestMain(m *testing.M) {
	h = New(Options{
		KeepCluster: os.Getenv("GOKUBEGATE_E2E_KEEP") == "1",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	if err := setupSuite(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "[e2e] setup: %v\n", err)
		cleanupSuite()
		os.Exit(1)
	}

	code := m.Run()
	cleanupSuite()
	os.Exit(code)
}

func setupSuite(ctx context.Context) error {
	if err := h.EnsureCluster(ctx); err != nil {
		return err
	}
	if err := h.BuildAndLoadImages(ctx); err != nil {
		return fmt.Errorf("build/load images: %w", err)
	}
	for _, f := range []string{"namespace.yaml", "rbac.yaml", "rbac-denied.yaml", "downstream.yaml"} {
		if err := h.Apply(ctx, h.repo("test/e2e/manifests/"+f)); err != nil {
			return fmt.Errorf("apply %s: %w", f, err)
		}
	}
	if err := h.WaitDeploymentReady(ctx, "downstream", 2); err != nil {
		return fmt.Errorf("downstream not ready: %w", err)
	}
	if err := h.WaitEndpointsReady(ctx, 2); err != nil {
		return fmt.Errorf("downstream endpoints not ready: %w", err)
	}
	return nil
}

func cleanupSuite() {
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cleanupCancel()
	if h.kube != nil {
		h.DeleteNamespace(cleanupCtx)
	}
	h.DeleteCluster(cleanupCtx)
}

func svcURL(path string) string {
	return fmt.Sprintf("http://%s.%s.svc.cluster.local:8080%s", h.opts.Service, h.opts.Namespace, path)
}

func ensureDownstream(t *testing.T, ctx context.Context, replicas int32) []string {
	t.Helper()
	if err := h.ScaleDeployment(ctx, "downstream", replicas); err != nil {
		t.Fatalf("scale downstream to %d: %v", replicas, err)
	}
	if err := h.WaitDeploymentReady(ctx, "downstream", replicas); err != nil {
		t.Fatalf("wait for %d ready downstream pods: %v", replicas, err)
	}
	if err := h.WaitEndpointsReady(ctx, int(replicas)); err != nil {
		t.Fatalf("wait for %d ready endpoints: %v", replicas, err)
	}
	pods, err := h.ReadyEndpointPodNames(ctx)
	if err != nil {
		t.Fatalf("list ready endpoint pods: %v", err)
	}
	if len(pods) != int(replicas) {
		t.Fatalf("want %d ready endpoint pods, got %v", replicas, pods)
	}
	return pods
}

func runTester(t *testing.T, ctx context.Context, name string, requests int, path string) TesterResult {
	t.Helper()
	logs, err := h.RunTesterJob(ctx, "tester-"+name, []string{
		"-phase", name,
		"-namespace", h.opts.Namespace,
		"-service", h.opts.Service,
		"-url", svcURL(path),
		"-requests", fmt.Sprintf("%d", requests),
	})
	if err != nil {
		t.Fatalf("tester job %s: %v", name, err)
	}
	res, err := ParseTesterResult(logs)
	if err != nil {
		t.Fatalf("parse tester result %s: %v", name, err)
	}
	if res.Success != requests || res.Errors != 0 {
		t.Fatalf("%s: want %d success/0 errors, got success=%d errors=%d", name, requests, res.Success, res.Errors)
	}
	qps := float64(0)
	if res.DurationMs > 0 {
		qps = float64(requests) * 1000 / float64(res.DurationMs)
	}
	t.Logf("phase=%s requests=%d success=%d errors=%d duration=%dms qps=%.1f reusedRatio=%.3f distribution=%v",
		name, requests, res.Success, res.Errors, res.DurationMs, qps, res.ReusedRatio, res.ByEndpoint)
	return res
}

func requireEndpointSet(t *testing.T, got map[string]int, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("endpoint distribution %v does not match ready pods %v", got, want)
	}
	for _, pod := range want {
		if got[pod] == 0 {
			t.Fatalf("ready pod %s received no traffic; distribution=%v", pod, got)
		}
	}
}

func runCustomTester(t *testing.T, ctx context.Context, jobName string, args []string) TesterResult {
	t.Helper()
	logs, err := h.RunTesterJob(ctx, jobName, args)
	if err != nil {
		t.Fatalf("tester job %s: %v", jobName, err)
	}
	res, err := ParseTesterResult(logs)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// TestE2EAPIServerPause (S7) verifies that request routing continues from the
// last-known-good snapshot while the kind control plane is unavailable.
func TestE2EAPIServerPause(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ensureDownstream(t, ctx, 2)

	const jobName = "tester-apiserver-pause"
	args := []string{
		"-phase", "apiserver-pause",
		"-namespace", h.opts.Namespace,
		"-service", h.opts.Service,
		"-url", svcURL("/"),
		"-concurrency", "32",
		"-duration", "15s",
	}
	if err := h.StartTesterJob(ctx, jobName, args); err != nil {
		t.Fatalf("start sustained tester: %v", err)
	}
	time.Sleep(3 * time.Second)
	if err := h.PauseControlPlane(ctx); err != nil {
		t.Fatal(err)
	}
	pausedAt := time.Now()
	defer func() { _ = h.ResumeControlPlane(ctx) }()
	time.Sleep(5 * time.Second)
	if err := h.ResumeControlPlane(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("API server unavailable for %s under sustained load", time.Since(pausedAt).Round(time.Millisecond))

	logs, err := h.WaitTesterJob(ctx, jobName)
	if err != nil {
		t.Fatalf("wait tester after API server recovery: %v", err)
	}
	res, err := ParseTesterResult(logs)
	if err != nil {
		t.Fatal(err)
	}
	if res.Errors != 0 || res.Success < 1_000 {
		t.Fatalf("last-known-good routing failed: requests=%d success=%d errors=%d kinds=%v", res.Requests, res.Success, res.Errors, res.ErrorKinds)
	}
	for i, window := range res.Windows {
		if i > 0 && i < len(res.Windows)-1 && window.Success == 0 {
			t.Fatalf("API pause caused a full-second outage: %+v", window)
		}
	}
	t.Logf("API pause summary: requests=%d errors=%d throughput=%.1f/s latency_ms[p50=%.2f p95=%.2f p99=%.2f max=%.2f]",
		res.Requests, res.Errors, res.Throughput, res.LatencyP50Ms, res.LatencyP95Ms, res.LatencyP99Ms, res.LatencyMaxMs)
}

// TestE2EBasicDistribution (S1): 500 requests across 2 pods must be split
// within tolerance, with connection reuse, and cross-checked by pod-side stats.
func TestE2EBasicDistribution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	const total = 500
	pods := ensureDownstream(t, ctx, 2)
	before := make(map[string]int64, len(pods))
	for _, pod := range pods {
		count, err := h.PodStats(ctx, pod)
		if err != nil {
			t.Fatalf("stats before run for %s: %v", pod, err)
		}
		before[pod] = count
	}

	res := runTester(t, ctx, "baseline", total, "/")
	requireEndpointSet(t, res.ByEndpoint, pods)
	for pod, n := range res.ByEndpoint {
		ratio := float64(n) / total
		if ratio < 0.4 || ratio > 0.6 {
			t.Fatalf("endpoint %s got %d (%.1f%%) — outside [40%%,60%%]", pod, n, ratio*100)
		}
	}
	if res.ReusedRatio < 0.8 {
		t.Fatalf("connection reuse ratio %.2f too low, want >= 0.8", res.ReusedRatio)
	}

	// Cross-check with authoritative pod-side counters.
	for _, pod := range pods {
		after, err := h.PodStats(ctx, pod)
		if err != nil {
			t.Fatalf("stats for %s: %v", pod, err)
		}
		delta := after - before[pod]
		if delta < total*2/5 || delta > total*3/5 {
			t.Fatalf("pod %s served %d requests in this phase — outside [40%%,60%%] of %d", pod, delta, total)
		}
		t.Logf("baseline pod-side count: pod=%s requests=%d", pod, delta)
	}
}

// TestE2EConcurrentRapidScaling keeps one client under 64-way sustained load
// while the Deployment is resized every two seconds. Per-second windows catch
// short blackouts that aggregate success rates could hide.
func TestE2EConcurrentRapidScaling(t *testing.T) {
	testConcurrentRapidScaling(t, "pod")
}

// TestE2EConcurrentRapidScalingClusterIP runs the same lifecycle pressure
// through the shared Service transport with the default 0.1% rotation.
func TestE2EConcurrentRapidScalingClusterIP(t *testing.T) {
	testConcurrentRapidScaling(t, "clusterip")
}

func testConcurrentRapidScaling(t *testing.T, mode string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	ensureDownstream(t, ctx, 2)
	defer ensureDownstream(t, ctx, 2)

	const concurrency = 64
	const loadTime = 24 * time.Second
	jobName := "tester-rapid-scaling-" + mode
	args := []string{
		"-phase", "rapid-scaling-" + mode,
		"-namespace", h.opts.Namespace,
		"-service", h.opts.Service,
		"-url", svcURL("/"),
		"-concurrency", fmt.Sprintf("%d", concurrency),
		"-duration", loadTime.String(),
		"-gate-mode", mode,
		"-connection-close-denominator", "1000",
	}
	if err := h.StartTesterJob(ctx, jobName, args); err != nil {
		t.Fatalf("start sustained tester: %v", err)
	}

	started := time.Now()
	timeline := []int32{8, 2, 6, 1, 4}
	for _, replicas := range timeline {
		time.Sleep(2 * time.Second)
		if err := h.ScaleDeployment(ctx, "downstream", replicas); err != nil {
			t.Fatalf("rapid scale to %d: %v", replicas, err)
		}
		t.Logf("rapid scaling command: mode=%s elapsed=%s replicas=%d", mode, time.Since(started).Round(time.Millisecond), replicas)
	}
	if err := h.WaitDeploymentReady(ctx, "downstream", 4); err != nil {
		t.Fatalf("wait for final four replicas: %v", err)
	}
	if err := h.WaitEndpointsReady(ctx, 4); err != nil {
		t.Fatalf("wait for final four endpoints: %v", err)
	}

	logs, err := h.WaitTesterJob(ctx, jobName)
	if err != nil {
		t.Fatalf("wait sustained tester: %v", err)
	}
	res, err := ParseTesterResult(logs)
	if err != nil {
		t.Fatal(err)
	}
	if res.Requests < 1_000 {
		t.Fatalf("sustained test generated only %d requests", res.Requests)
	}
	errorRatio := float64(res.Errors) / float64(res.Requests)
	if errorRatio > 0.01 {
		t.Fatalf("rapid scaling error ratio %.3f%% exceeds 1%%: errors=%v samples=%v", errorRatio*100, res.ErrorKinds, res.ErrorSamples)
	}
	if res.LatencyP99Ms > 2_000 {
		t.Fatalf("rapid scaling p99 %.1fms exceeds 2s", res.LatencyP99Ms)
	}
	distribution := res.ByEndpoint
	if mode == "clusterip" {
		distribution = res.ByPod
	}
	if len(distribution) < 4 {
		t.Fatalf("mode %s only reached %d backends during scaling: %v", mode, len(distribution), distribution)
	}

	maxWindowErrorRatio := float64(0)
	firstSeen := make(map[string]int)
	for i, window := range res.Windows {
		// The first and final windows may be partial; all complete windows must
		// retain successful traffic, even during EndpointSlice transitions.
		complete := i > 0 && i < len(res.Windows)-1
		if complete && window.Success == 0 {
			t.Fatalf("complete second %d had no successful requests: %+v", window.Second, window)
		}
		windowErrorRatio := float64(0)
		if window.Requests > 0 {
			windowErrorRatio = float64(window.Errors) / float64(window.Requests)
		}
		if windowErrorRatio > maxWindowErrorRatio {
			maxWindowErrorRatio = windowErrorRatio
		}
		for pod, count := range window.ByPod {
			if count > 0 {
				if _, ok := firstSeen[pod]; !ok {
					firstSeen[pod] = window.Second
				}
			}
		}
		t.Logf("load window: second=%d requests=%d success=%d errors=%d p95=%.2fms byPod=%v",
			window.Second, window.Requests, window.Success, window.Errors, window.LatencyP95Ms, window.ByPod)
	}
	t.Logf("rapid scaling summary: mode=%s requests=%d success=%d errors=%d errorRatio=%.4f%% throughput=%.1f/s reuse=%.3f rotations=%d latency_ms[p50=%.2f p95=%.2f p99=%.2f max=%.2f] maxWindowErrorRatio=%.4f%% backends=%d distribution=%v firstSeen=%v errorKinds=%v endpointUpdates=%+v",
		mode,
		res.Requests, res.Success, res.Errors, errorRatio*100, res.Throughput, res.ReusedRatio,
		res.Rotations,
		res.LatencyP50Ms, res.LatencyP95Ms, res.LatencyP99Ms, res.LatencyMaxMs,
		maxWindowErrorRatio*100, len(distribution), distribution, firstSeen, res.ErrorKinds, res.EndpointUpdates)
	if res.Errors > 0 {
		t.Logf("rapid scaling error samples: %v", res.ErrorSamples)
	}
}

// TestE2EConcurrencyGradient records latency and throughput under increasing
// concurrency on a stable four-pod endpoint set.
func TestE2EConcurrencyGradient(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	pods := ensureDownstream(t, ctx, 4)
	defer ensureDownstream(t, ctx, 2)

	for _, concurrency := range []int{1, 8, 32, 64} {
		name := fmt.Sprintf("concurrency-%d", concurrency)
		res := runCustomTester(t, ctx, "tester-"+name, []string{
			"-phase", name,
			"-namespace", h.opts.Namespace,
			"-service", h.opts.Service,
			"-url", svcURL("/"),
			"-concurrency", fmt.Sprintf("%d", concurrency),
			"-duration", "4s",
		})
		if res.Errors != 0 || res.Success < 100 {
			t.Fatalf("concurrency %d: requests=%d success=%d errors=%d kinds=%v", concurrency, res.Requests, res.Success, res.Errors, res.ErrorKinds)
		}
		requireEndpointSet(t, res.ByEndpoint, pods)
		if res.LatencyP99Ms > 2_000 {
			t.Fatalf("concurrency %d p99 %.2fms exceeds 2s", concurrency, res.LatencyP99Ms)
		}
		t.Logf("concurrency gradient: concurrency=%d requests=%d throughput=%.1f/s reuse=%.3f latency_ms[p50=%.2f p95=%.2f p99=%.2f max=%.2f]",
			concurrency, res.Requests, res.Throughput, res.ReusedRatio,
			res.LatencyP50Ms, res.LatencyP95Ms, res.LatencyP99Ms, res.LatencyMaxMs)
	}
}

// TestE2ESSEAffinity (S6) opens concurrent long-lived streams. Each stream's
// tester-side parser fails if events ever identify more than one pod.
func TestE2ESSEAffinity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	pods := ensureDownstream(t, ctx, 4)
	defer ensureDownstream(t, ctx, 2)

	const streams = 20
	res := runCustomTester(t, ctx, "tester-sse", []string{
		"-phase", "sse",
		"-namespace", h.opts.Namespace,
		"-service", h.opts.Service,
		"-url", svcURL("/stream?duration=2s&interval=50ms"),
		"-mode", "sse",
		"-requests", fmt.Sprintf("%d", streams),
		"-concurrency", fmt.Sprintf("%d", streams),
	})
	if res.Success != streams || res.Errors != 0 {
		t.Fatalf("SSE affinity failed: success=%d errors=%d kinds=%v", res.Success, res.Errors, res.ErrorKinds)
	}
	requireEndpointSet(t, res.ByEndpoint, pods)
	for pod, count := range res.ByEndpoint {
		if count != streams/len(pods) {
			t.Fatalf("SSE pod %s got %d streams, want %d; distribution=%v", pod, count, streams/len(pods), res.ByEndpoint)
		}
	}
	t.Logf("SSE summary: streams=%d duration=%dms success=%d distribution=%v", streams, res.DurationMs, res.Success, res.ByEndpoint)
}

// TestE2EHostAndPathPreservation (S2): downstream must see the logical service
// Host by default and the user-provided Host when set; path/query must arrive
// unchanged.
func TestE2EHostAndPathPreservation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ensureDownstream(t, ctx, 2)

	logicalHost := fmt.Sprintf("%s.%s.svc.cluster.local:8080", h.opts.Service, h.opts.Namespace)
	url := svcURL("/api/items?page=1")

	run := func(name string, host string, wantHost string) {
		t.Helper()
		args := []string{
			"-phase", name,
			"-namespace", h.opts.Namespace,
			"-service", h.opts.Service,
			"-url", url,
			"-requests", "50",
		}
		if host != "" {
			args = append(args, "-host", host)
		}
		logs, err := h.RunTesterJob(ctx, "tester-"+name, args)
		if err != nil {
			t.Fatalf("tester job %s: %v", name, err)
		}
		res, err := ParseTesterResult(logs)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		if res.Errors != 0 || res.Success != 50 {
			t.Fatalf("%s: want 50 success/0 errors, got %d/%d", name, res.Success, res.Errors)
		}
		if res.HostEcho != wantHost {
			t.Fatalf("%s: Host echo %q, want %q", name, res.HostEcho, wantHost)
		}
		if res.PathEcho != "/api/items" || res.QueryEcho != "page=1" {
			t.Fatalf("%s: path/query echo %q?%q, want /api/items?page=1", name, res.PathEcho, res.QueryEcho)
		}
		t.Logf("phase=%s host=%q path=%q query=%q", name, res.HostEcho, res.PathEcho, res.QueryEcho)
	}

	run("host-default", "", logicalHost)
	run("host-custom", "custom.example.com:8080", "custom.example.com:8080")
}

// TestE2EModeComparison contrasts deterministic Pod routing with a single
// ClusterIP keep-alive pool, both without and with sampled connection rotation.
// It covers sequential traffic (where L4 stickiness is strongest) and 64-way
// concurrency (where the shared Transport naturally opens multiple connections).
func TestE2EModeComparison(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	pods := ensureDownstream(t, ctx, 4)
	defer ensureDownstream(t, ctx, 2)

	const requests = 8_000
	runMode := func(name, mode string, denominator uint64) TesterResult {
		t.Helper()
		return runCustomTester(t, ctx, "tester-"+name, []string{
			"-phase", name,
			"-namespace", h.opts.Namespace,
			"-service", h.opts.Service,
			"-url", svcURL("/"),
			"-requests", fmt.Sprintf("%d", requests),
			"-gate-mode", mode,
			"-connection-close-denominator", fmt.Sprintf("%d", denominator),
		})
	}

	podMode := runMode("mode-pod", "pod", 1000)
	if podMode.Success != requests || podMode.Errors != 0 {
		t.Fatalf("pod mode: success=%d errors=%d", podMode.Success, podMode.Errors)
	}
	requireEndpointSet(t, podMode.ByPod, pods)
	for pod, count := range podMode.ByPod {
		if count != requests/len(pods) {
			t.Fatalf("pod mode distribution is not exact: pod=%s count=%d distribution=%v", pod, count, podMode.ByPod)
		}
	}
	if podMode.Rotations != 0 {
		t.Fatalf("pod mode emitted %d ClusterIP rotations", podMode.Rotations)
	}

	sticky := runMode("mode-clusterip-sticky", "clusterip", 0)
	if sticky.Success != requests || sticky.Errors != 0 {
		t.Fatalf("sticky clusterip: success=%d errors=%d", sticky.Success, sticky.Errors)
	}
	if len(sticky.ByPod) != 1 {
		t.Fatalf("ClusterIP without rotation should stay on one keep-alive backend, got %v", sticky.ByPod)
	}
	if sticky.Rotations != 0 {
		t.Fatalf("disabled ClusterIP rotation emitted %d events", sticky.Rotations)
	}

	rotated := runMode("mode-clusterip-rotated", "clusterip", 1000)
	if rotated.Success != requests || rotated.Errors != 0 {
		t.Fatalf("rotated clusterip: success=%d errors=%d", rotated.Success, rotated.Errors)
	}
	if rotated.Rotations == 0 {
		t.Fatal("ClusterIP mode sampled no connection rotations")
	}
	if len(rotated.ByPod) < 2 {
		t.Fatalf("ClusterIP rotation did not improve backend spread: %v", rotated.ByPod)
	}
	if rotated.ReusedRatio >= sticky.ReusedRatio {
		t.Fatalf("rotation did not reduce connection reuse: rotated=%.4f sticky=%.4f", rotated.ReusedRatio, sticky.ReusedRatio)
	}

	t.Logf("mode comparison pod: distribution=%v reuse=%.4f throughput=%.1f/s p99=%.2fms",
		podMode.ByPod, podMode.ReusedRatio, podMode.Throughput, podMode.LatencyP99Ms)
	t.Logf("mode comparison clusterip no-rotation: distribution=%v reuse=%.4f throughput=%.1f/s p99=%.2fms",
		sticky.ByPod, sticky.ReusedRatio, sticky.Throughput, sticky.LatencyP99Ms)
	t.Logf("mode comparison clusterip rotated: denominator=1000 rotations=%d distribution=%v reuse=%.4f throughput=%.1f/s p99=%.2fms",
		rotated.Rotations, rotated.ByPod, rotated.ReusedRatio, rotated.Throughput, rotated.LatencyP99Ms)

	runConcurrent := func(name, mode string, denominator uint64) TesterResult {
		t.Helper()
		res := runCustomTester(t, ctx, "tester-"+name, []string{
			"-phase", name,
			"-namespace", h.opts.Namespace,
			"-service", h.opts.Service,
			"-url", svcURL("/"),
			"-duration", "5s",
			"-concurrency", "64",
			"-gate-mode", mode,
			"-connection-close-denominator", fmt.Sprintf("%d", denominator),
		})
		if res.Errors != 0 || res.Success < 1_000 {
			t.Fatalf("%s: requests=%d success=%d errors=%d samples=%v", name, res.Requests, res.Success, res.Errors, res.ErrorSamples)
		}
		return res
	}

	concurrentPod := runConcurrent("mode-concurrent-pod", "pod", 1000)
	requireEndpointSet(t, concurrentPod.ByPod, pods)
	minCount, maxCount := distributionRange(concurrentPod.ByPod)
	if maxCount-minCount > 1 {
		t.Fatalf("concurrent pod mode is not exact: %v", concurrentPod.ByPod)
	}
	concurrentSticky := runConcurrent("mode-concurrent-clusterip-sticky", "clusterip", 0)
	if len(concurrentSticky.ByPod) < 2 {
		t.Fatalf("64-way ClusterIP traffic did not establish connections to multiple pods: %v", concurrentSticky.ByPod)
	}
	concurrentRotated := runConcurrent("mode-concurrent-clusterip-rotated", "clusterip", 1000)
	if len(concurrentRotated.ByPod) < 2 || concurrentRotated.Rotations == 0 {
		t.Fatalf("concurrent ClusterIP rotation ineffective: rotations=%d distribution=%v", concurrentRotated.Rotations, concurrentRotated.ByPod)
	}

	for _, result := range []TesterResult{concurrentPod, concurrentSticky, concurrentRotated} {
		minCount, maxCount := distributionRange(result.ByPod)
		skew := float64(maxCount-minCount) / float64(result.Success)
		t.Logf("mode comparison concurrent: mode=%s rotations=%d requests=%d distribution=%v skew=%.4f reuse=%.4f throughput=%.1f/s latency_ms[p95=%.2f p99=%.2f]",
			result.Mode, result.Rotations, result.Requests, result.ByPod, skew, result.ReusedRatio,
			result.Throughput, result.LatencyP95Ms, result.LatencyP99Ms)
	}
}

// TestE2EModeBenchmark compares every routing mode and parameter combination
// under sustained load aimed at 5,000 QPS per downstream Pod (1c1g) for
// requests completing within 10 ms. Per-Pod QPS is read from authoritative
// pod-side counters; latency/error/connection metrics come from the tester.
// It also measures 2 -> 4 scale-up convergence for each ClusterIP rotation
// rate, where slower rates must rely on new connections reaching new Pods.
func TestE2EModeBenchmark(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	pods := ensureDownstream(t, ctx, 4)
	defer ensureDownstream(t, ctx, 2)

	const (
		targetQPSPerPod = 5_000
		concurrency     = 128
		loadDuration    = 12 * time.Second
		rateDuration    = 10 * time.Second
	)

	type benchConfig struct {
		name        string
		mode        string
		denominator uint64
		maxIdle     int
	}

	steadyState := []benchConfig{
		{name: "pod-idle8", mode: "pod", denominator: 0, maxIdle: 8},
		{name: "pod-idle16", mode: "pod", denominator: 0, maxIdle: 16},
		{name: "pod-idle32", mode: "pod", denominator: 0, maxIdle: 32},
		{name: "pod-idle64", mode: "pod", denominator: 0, maxIdle: 64},
		{name: "pod-idle128", mode: "pod", denominator: 0, maxIdle: 128},
		{name: "pod-idle512", mode: "pod", denominator: 0, maxIdle: 512},
		{name: "clusterip-denom0", mode: "clusterip", denominator: 0, maxIdle: 16},
		{name: "clusterip-denom1000", mode: "clusterip", denominator: 1000, maxIdle: 16},
		{name: "clusterip-denom500", mode: "clusterip", denominator: 500, maxIdle: 16},
		{name: "clusterip-denom200", mode: "clusterip", denominator: 200, maxIdle: 16},
		{name: "clusterip-denom100", mode: "clusterip", denominator: 100, maxIdle: 16},
	}

	targetTotal := targetQPSPerPod * len(pods)
	// GOKUBEGATE_E2E_BENCH_RATE_ONLY=1 runs only the rate-limited acceptance
	// phases; saturation and convergence are captured by the default full run.
	rateOnly := os.Getenv("GOKUBEGATE_E2E_BENCH_RATE_ONLY") == "1"

	if !rateOnly {
		for _, cfg := range steadyState {
			t.Run("bench-"+cfg.name, func(t *testing.T) {
				before := make(map[string]int64, len(pods))
				for _, pod := range pods {
					count, err := h.PodStats(ctx, pod)
					if err != nil {
						t.Fatalf("stats before for %s: %v", pod, err)
					}
					before[pod] = count
				}

				res := runCustomTester(t, ctx, "tester-bench-"+cfg.name, []string{
					"-phase", "bench-" + cfg.name,
					"-namespace", h.opts.Namespace,
					"-service", h.opts.Service,
					"-url", svcURL("/"),
					"-concurrency", fmt.Sprintf("%d", concurrency),
					"-duration", loadDuration.String(),
					"-gate-mode", cfg.mode,
					"-connection-close-denominator", fmt.Sprintf("%d", cfg.denominator),
					"-max-idle-conns", fmt.Sprintf("%d", cfg.maxIdle),
				})

				after := make(map[string]int64, len(pods))
				for _, pod := range pods {
					count, err := h.PodStats(ctx, pod)
					if err != nil {
						t.Fatalf("stats after for %s: %v", pod, err)
					}
					after[pod] = count
				}

				perPodQPS := make(map[string]float64, len(pods))
				totalPodQPS := 0.0
				for _, pod := range pods {
					qps := float64(after[pod]-before[pod]) / loadDuration.Seconds()
					perPodQPS[pod] = qps
					totalPodQPS += qps
				}

				reachedTarget := totalPodQPS >= float64(targetTotal)
				metLatency := res.LatencyP99Ms <= 10
				status := "MET"
				switch {
				case !reachedTarget:
					status = "NOT-MET(qps)"
				case !metLatency:
					status = "NOT-MET(p99)"
				}
				minQPS, maxQPS := 0.0, 0.0
				first := true
				for _, qps := range perPodQPS {
					if first || qps < minQPS {
						minQPS = qps
					}
					if first || qps > maxQPS {
						maxQPS = qps
					}
					first = false
				}
				t.Logf("bench %s: status=%s totalQPS=%.0f target=%d avgPerPod=%.0f minPod=%.0f maxPod=%.0f errors=%d errorKinds=%v p50=%.2f p95=%.2f p99=%.2f max=%.2fms reuse=%.4f connections=%d rotations=%d backends=%d distribution=%v",
					cfg.name, status, totalPodQPS, targetTotal, totalPodQPS/float64(len(pods)),
					minQPS, maxQPS,
					res.Errors, res.ErrorKinds,
					res.LatencyP50Ms, res.LatencyP95Ms, res.LatencyP99Ms, res.LatencyMaxMs,
					res.ReusedRatio, res.Connections, res.Rotations, len(res.ByPod), res.ByPod)
			})
		}
	}

	// Rate-limited acceptance: sustain exactly targetTotal req/s and require
	// zero errors, p99 <= 10 ms, and each Pod receiving roughly its share.
	rateMet := 0
	for _, cfg := range steadyState {
		t.Run("rate-"+cfg.name, func(t *testing.T) {
			before := make(map[string]int64, len(pods))
			for _, pod := range pods {
				count, err := h.PodStats(ctx, pod)
				if err != nil {
					t.Fatalf("stats before for %s: %v", pod, err)
				}
				before[pod] = count
			}

			res := runCustomTester(t, ctx, "tester-rate-"+cfg.name, []string{
				"-phase", "rate-" + cfg.name,
				"-namespace", h.opts.Namespace,
				"-service", h.opts.Service,
				"-url", svcURL("/"),
				"-concurrency", fmt.Sprintf("%d", concurrency),
				"-duration", rateDuration.String(),
				"-rate", fmt.Sprintf("%d", targetTotal),
				"-gate-mode", cfg.mode,
				"-connection-close-denominator", fmt.Sprintf("%d", cfg.denominator),
				"-max-idle-conns", fmt.Sprintf("%d", cfg.maxIdle),
			})

			after := make(map[string]int64, len(pods))
			for _, pod := range pods {
				count, err := h.PodStats(ctx, pod)
				if err != nil {
					t.Fatalf("stats after for %s: %v", pod, err)
				}
				after[pod] = count
			}

			achieved := res.Throughput
			metRate := achieved >= float64(targetTotal)*0.95
			metLatency := res.LatencyP99Ms <= 10
			balanced := true
			perPodQPS := make(map[string]float64, len(pods))
			for _, pod := range pods {
				qps := float64(after[pod]-before[pod]) / rateDuration.Seconds()
				perPodQPS[pod] = qps
				if qps < float64(targetQPSPerPod)*0.7 || qps > float64(targetQPSPerPod)*1.3 {
					balanced = false
				}
			}
			status := "MET"
			switch {
			case !metRate:
				status = "NOT-MET(rate)"
			case !metLatency:
				status = "NOT-MET(p99)"
			case !balanced:
				status = "NOT-MET(balance)"
			}
			if res.Errors != 0 {
				status = "NOT-MET(errors)"
			}
			if cfg.mode == "pod" && res.Errors == 0 && metRate && metLatency && balanced {
				rateMet++
			}
			t.Logf("rate %s: status=%s achieved=%.0f req/s target=%d perPodQPS=%v p50=%.2f p95=%.2f p99=%.2f max=%.2fms reuse=%.4f connections=%d rotations=%d backends=%d distribution=%v",
				cfg.name, status, achieved, targetTotal, perPodQPS,
				res.LatencyP50Ms, res.LatencyP95Ms, res.LatencyP99Ms, res.LatencyMaxMs,
				res.ReusedRatio, res.Connections, res.Rotations, len(res.ByPod), res.ByPod)
			if cfg.mode == "pod" && (res.Errors != 0 || !metRate || !metLatency || !balanced) {
				t.Fatalf("pod mode %s: %s errors=%d errorKinds=%v samples=%v", cfg.name, status, res.Errors, res.ErrorKinds, res.ErrorSamples)
			}
		})
	}
	if rateMet == 0 {
		t.Fatalf("no pod-mode configuration met %d QPS total (%d per Pod) with p99 <= 10ms and balanced per-Pod traffic", targetTotal, targetQPSPerPod)
	}

	if !rateOnly {
		// Scale-up convergence: how quickly do the two new Pods receive traffic
		// after 2 -> 4, per ClusterIP rotation rate.
		convergence := []benchConfig{
			{name: "clusterip-denom0", mode: "clusterip", denominator: 0, maxIdle: 16},
			{name: "clusterip-denom1000", mode: "clusterip", denominator: 1000, maxIdle: 16},
			{name: "clusterip-denom500", mode: "clusterip", denominator: 500, maxIdle: 16},
			{name: "clusterip-denom200", mode: "clusterip", denominator: 200, maxIdle: 16},
			{name: "clusterip-denom100", mode: "clusterip", denominator: 100, maxIdle: 16},
		}
		for _, cfg := range convergence {
			t.Run("converge-"+cfg.name, func(t *testing.T) {
				initial := ensureDownstream(t, ctx, 2)
				jobName := "tester-converge-" + cfg.name
				args := []string{
					"-phase", "converge-" + cfg.name,
					"-namespace", h.opts.Namespace,
					"-service", h.opts.Service,
					"-url", svcURL("/"),
					"-concurrency", fmt.Sprintf("%d", concurrency),
					"-duration", "18s",
					"-gate-mode", cfg.mode,
					"-connection-close-denominator", fmt.Sprintf("%d", cfg.denominator),
					"-max-idle-conns", fmt.Sprintf("%d", cfg.maxIdle),
				}
				if err := h.StartTesterJob(ctx, jobName, args); err != nil {
					t.Fatalf("start convergence tester: %v", err)
				}
				startedAt := time.Now()
				time.Sleep(2 * time.Second)
				if err := h.ScaleDeployment(ctx, "downstream", 4); err != nil {
					t.Fatalf("scale to 4: %v", err)
				}
				readyAt := time.Now()
				if err := h.WaitEndpointsReady(ctx, 4); err != nil {
					t.Fatalf("wait 4 endpoints: %v", err)
				}
				logs, err := h.WaitTesterJob(ctx, jobName)
				if err != nil {
					t.Fatalf("wait convergence tester: %v", err)
				}
				res, err := ParseTesterResult(logs)
				if err != nil {
					t.Fatal(err)
				}
				finalPods, err := h.ReadyEndpointPodNames(ctx)
				if err != nil {
					t.Fatal(err)
				}
				initialSet := make(map[string]bool, len(initial))
				for _, pod := range initial {
					initialSet[pod] = true
				}
				var newPods []string
				for _, pod := range finalPods {
					if !initialSet[pod] {
						newPods = append(newPods, pod)
					}
				}
				firstSeen := make(map[string]int)
				for _, window := range res.Windows {
					for pod := range window.ByPod {
						if _, ok := firstSeen[pod]; !ok {
							firstSeen[pod] = window.Second
						}
					}
				}
				var delays []string
				for _, pod := range newPods {
					sec, ok := firstSeen[pod]
					if !ok {
						delays = append(delays, pod+"=never")
						continue
					}
					delayMs := startedAt.Add(time.Duration(sec) * time.Second).Sub(readyAt).Milliseconds()
					delays = append(delays, fmt.Sprintf("%s=%dms", pod, delayMs))
				}
				t.Logf("convergence %s: newPods=%v firstSeen=%v delayAfterReady=%v requests=%d errors=%d errorKinds=%v rotations=%d reuse=%.4f distribution=%v",
					cfg.name, newPods, firstSeen, delays, res.Requests, res.Errors, res.ErrorKinds, res.Rotations, res.ReusedRatio, res.ByPod)
			})
		}
	}
}

// TestE2ERolloutReplacement simulates a rolling release: 10 running Pods, 10
// replacement Pods start and become ready, then the old 10 are removed
// (maxSurge=100%, maxUnavailable=0). Sustained load must keep flowing with a
// low error rate and no full-second outage while the new Pods receive traffic.
func TestE2ERolloutReplacement(t *testing.T) {
	testRolloutReplacement(t, "pod")
}

// TestE2ERolloutReplacementClusterIP runs the same rollout replacement through
// the shared Service transport with the default 1/1000 rotation.
func TestE2ERolloutReplacementClusterIP(t *testing.T) {
	testRolloutReplacement(t, "clusterip")
}

func testRolloutReplacement(t *testing.T, mode string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	oldPods := ensureDownstream(t, ctx, 10)
	defer ensureDownstream(t, ctx, 2)

	const concurrency = 128
	const loadTime = 120 * time.Second
	jobName := "tester-rollout-" + mode
	args := []string{
		"-phase", "rollout-" + mode,
		"-namespace", h.opts.Namespace,
		"-service", h.opts.Service,
		"-url", svcURL("/"),
		"-concurrency", fmt.Sprintf("%d", concurrency),
		"-duration", loadTime.String(),
		"-gate-mode", mode,
		"-connection-close-denominator", "1000",
	}
	if err := h.StartTesterJob(ctx, jobName, args); err != nil {
		t.Fatalf("start rollout tester: %v", err)
	}
	time.Sleep(3 * time.Second)

	// Zero-downtime rolling replacement: the new ReplicaSet must become fully
	// ready before the old Pods are removed. Use a unique template label per
	// invocation so repeated runs always trigger a new ReplicaSet.
	patch := fmt.Sprintf(`{"spec":{"strategy":{"type":"RollingUpdate","rollingUpdate":{"maxSurge":"100%%","maxUnavailable":"0%%"}},"template":{"metadata":{"labels":{"rollout":%q}}}}}`,
		fmt.Sprintf("r-%s-%d", mode, time.Now().UnixNano()))
	if _, err := h.run(ctx, h.kubect, "--kubeconfig", h.kubeconfigPath(),
		"patch", "deployment/downstream", "-n", h.opts.Namespace, "--type", "strategic", "-p", patch); err != nil {
		t.Fatalf("patch rollout: %v", err)
	}
	if _, err := h.run(ctx, h.kubect, "--kubeconfig", h.kubeconfigPath(),
		"rollout", "status", "deployment/downstream", "-n", h.opts.Namespace, "--timeout=3m"); err != nil {
		t.Fatalf("rollout status: %v", err)
	}

	logs, err := h.WaitTesterJob(ctx, jobName)
	if err != nil {
		t.Fatalf("wait rollout tester: %v", err)
	}
	res, err := ParseTesterResult(logs)
	if err != nil {
		t.Fatal(err)
	}

	finalPods, err := h.ReadyEndpointPodNames(ctx)
	if err != nil {
		t.Fatal(err)
	}
	oldSet := make(map[string]bool, len(oldPods))
	for _, pod := range oldPods {
		oldSet[pod] = true
	}
	var newPods []string
	for _, pod := range finalPods {
		if !oldSet[pod] {
			newPods = append(newPods, pod)
		}
	}
	if len(newPods) != 10 {
		t.Fatalf("expected 10 new Pods after rollout, got %v", newPods)
	}

	errorRatio := float64(res.Errors) / float64(res.Requests)
	maxErrorRatio := 0.001
	if mode == "clusterip" {
		maxErrorRatio = 0.005
	}
	if errorRatio > maxErrorRatio {
		t.Fatalf("rollout %s error ratio %.4f%% exceeds %.1f%%: errors=%d kinds=%v samples=%v", mode, errorRatio*100, maxErrorRatio*100, res.Errors, res.ErrorKinds, res.ErrorSamples)
	}
	for i, window := range res.Windows {
		if i > 0 && i < len(res.Windows)-1 && window.Success == 0 {
			t.Fatalf("complete second %d had no successful requests: %+v", window.Second, window)
		}
	}

	firstSeen := make(map[string]int)
	for _, window := range res.Windows {
		for pod := range window.ByPod {
			if _, ok := firstSeen[pod]; !ok {
				firstSeen[pod] = window.Second
			}
		}
	}
	for _, pod := range newPods {
		if _, ok := firstSeen[pod]; !ok {
			t.Fatalf("new pod %s received no traffic during rollout; firstSeen=%v", pod, firstSeen)
		}
	}

	t.Logf("rollout %s summary: requests=%d errors=%d errorRatio=%.4f%% throughput=%.1f/s p50=%.2f p95=%.2f p99=%.2f max=%.2fms reuse=%.4f rotations=%d oldPods=%d newPods=%d firstSeen=%v backends=%d distribution=%v errorKinds=%v",
		mode, res.Requests, res.Errors, errorRatio*100, res.Throughput,
		res.LatencyP50Ms, res.LatencyP95Ms, res.LatencyP99Ms, res.LatencyMaxMs,
		res.ReusedRatio, res.Rotations, len(oldPods), len(newPods), firstSeen, len(res.ByPod), res.ByPod, res.ErrorKinds)
}

func distributionRange(distribution map[string]int) (minCount, maxCount int) {
	first := true
	for _, count := range distribution {
		if first || count < minCount {
			minCount = count
		}
		if first || count > maxCount {
			maxCount = count
		}
		first = false
	}
	return minCount, maxCount
}

// TestE2EScaleUp (S3): newly-ready pods must enter the picker after scaling
// from two replicas to four.
func TestE2EScaleUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ensureDownstream(t, ctx, 2)
	defer ensureDownstream(t, ctx, 2)

	pods := ensureDownstream(t, ctx, 4)
	const total = 500
	res := runTester(t, ctx, "scale-up", total, "/")
	requireEndpointSet(t, res.ByEndpoint, pods)
	for pod, count := range res.ByEndpoint {
		if count < total/20 {
			t.Fatalf("pod %s received %d requests after scale-up, want at least %d", pod, count, total/20)
		}
	}
}

// TestE2EScaleDown (S4-A): pods removed by a scale-down must not receive any
// new traffic once EndpointSlice propagation has completed.
func TestE2EScaleDown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ensureDownstream(t, ctx, 4)

	survivors := ensureDownstream(t, ctx, 2)
	res := runTester(t, ctx, "scale-down", 300, "/")
	requireEndpointSet(t, res.ByEndpoint, survivors)
}

// TestE2ENotReady (S4-B): a live but NotReady pod is removed from new picks,
// then re-enters rotation after readiness is restored.
func TestE2ENotReady(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	pods := ensureDownstream(t, ctx, 4)
	defer ensureDownstream(t, ctx, 2)

	victim := pods[0]
	if err := h.SetPodReady(ctx, victim, false); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.SetPodReady(ctx, victim, true) }()
	if err := h.WaitEndpointsReady(ctx, 3); err != nil {
		t.Fatalf("wait for NotReady endpoint removal: %v", err)
	}
	before, err := h.PodStats(ctx, victim)
	if err != nil {
		t.Fatalf("victim stats before run: %v", err)
	}
	res := runTester(t, ctx, "not-ready", 300, "/")
	if count := res.ByEndpoint[victim]; count != 0 {
		t.Fatalf("NotReady pod %s was picked %d times; distribution=%v", victim, count, res.ByEndpoint)
	}
	readyPods, err := h.ReadyEndpointPodNames(ctx)
	if err != nil {
		t.Fatalf("list ready endpoint pods: %v", err)
	}
	requireEndpointSet(t, res.ByEndpoint, readyPods)
	after, err := h.PodStats(ctx, victim)
	if err != nil {
		t.Fatalf("victim stats after run: %v", err)
	}
	if after != before {
		t.Fatalf("NotReady pod %s counter changed from %d to %d", victim, before, after)
	}
	t.Logf("NotReady pod=%s counter stayed at %d while 300 requests used %v", victim, before, res.ByEndpoint)

	if err := h.SetPodReady(ctx, victim, true); err != nil {
		t.Fatal(err)
	}
	if err := h.WaitEndpointsReady(ctx, 4); err != nil {
		t.Fatalf("wait for readiness restoration: %v", err)
	}
	rejoined := runTester(t, ctx, "ready-restored", 200, "/")
	if rejoined.ByEndpoint[victim] == 0 {
		t.Fatalf("restored pod %s did not re-enter rotation; distribution=%v", victim, rejoined.ByEndpoint)
	}
	t.Logf("restored pod=%s received=%d distribution=%v", victim, rejoined.ByEndpoint[victim], rejoined.ByEndpoint)
}

// TestE2ERBACDenied (S5): an unbound ServiceAccount must fail NewClient at
// startup with the API server's authorization error.
func TestE2ERBACDenied(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ensureDownstream(t, ctx, 2)

	started := time.Now()
	logs, err := h.RunTesterJobExpectFailure(ctx, "tester-rbac-denied", "gokubegate-tester-denied", []string{
		"-phase", "rbac-denied",
		"-namespace", h.opts.Namespace,
		"-service", h.opts.Service,
		"-url", svcURL("/"),
		"-requests", "1",
	})
	if err != nil {
		t.Fatalf("run denied tester: %v", err)
	}
	if !strings.Contains(strings.ToLower(logs), "forbidden") {
		t.Fatalf("denied tester logs do not contain a forbidden error: %s", logs)
	}
	if !strings.Contains(logs, "NewClient") {
		t.Fatalf("denied tester did not fail during NewClient startup: %s", logs)
	}
	t.Logf("denied startup failed after %s with log: %s", time.Since(started).Round(time.Millisecond), strings.TrimSpace(logs))
}

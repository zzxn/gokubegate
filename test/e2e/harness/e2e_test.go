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

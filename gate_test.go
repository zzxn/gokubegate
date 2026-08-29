package gokubegate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
)

func testServiceObject() *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: testService, Namespace: testNS},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Protocol:   corev1.ProtocolTCP,
				Port:       80,
				TargetPort: intstr.FromInt32(testPort),
			}},
		},
	}
}

func serviceWithTargetPort(port int32) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: testService, Namespace: testNS},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Protocol:   corev1.ProtocolTCP,
				Port:       80,
				TargetPort: intstr.FromInt32(port),
			}},
		},
	}
}

func newFakeGate(t *testing.T, objs ...runtime.Object) *Gate {
	t.Helper()
	cs := fake.NewSimpleClientset(objs...)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	gate, err := NewGate(ctx, testNS, testService,
		WithClientset(cs),
		WithCacheSyncTimeout(5*time.Second),
		WithDrainTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("NewGate: %v", err)
	}
	t.Cleanup(func() { _ = gate.Close() })
	return gate
}

func TestGateEndToEnd(t *testing.T) {
	// Two fake pods share one port on different loopback IPs so that a single
	// client config (one target port) can balance across both.
	var hits1, hits2 atomic.Int32
	var pathSeen sync.Map

	l1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := l1.Addr().(*net.TCPAddr).Port

	addr1 := "127.0.0.1"
	addr2 := "127.0.0.2"
	l2, err := net.Listen("tcp", fmt.Sprintf("%s:%d", addr2, port))
	if err != nil {
		t.Skipf("cannot bind %s:%d for second fake pod: %v", addr2, port, err)
	}

	srv1 := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits1.Add(1)
		pathSeen.Store(r.URL.Path+"?"+r.URL.RawQuery, true)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(r.Host))
	})}
	srv2 := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits2.Add(1)
		pathSeen.Store(r.URL.Path+"?"+r.URL.RawQuery, true)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(r.Host))
	})}
	go func() { _ = srv1.Serve(l1) }()
	go func() { _ = srv2.Serve(l2) }()
	t.Cleanup(func() { _ = srv1.Close() })
	t.Cleanup(func() { _ = srv2.Close() })

	objs := []runtime.Object{
		serviceWithTargetPort(int32(port)),
		testEndpointSlice("slice-1", []discoveryv1.Endpoint{
			{Addresses: []string{addr1}, TargetRef: podRef("pod-1", "uid-1")},
			{Addresses: []string{addr2}, TargetRef: podRef("pod-2", "uid-2")},
		}, testPorts(int32(port))),
	}

	gate := newFakeGate(t, objs...)
	if got := len(gate.Endpoints()); got != 2 {
		t.Fatalf("expected 2 ready endpoints, got %d", got)
	}

	client := &http.Client{Transport: gate, Timeout: 5 * time.Second}
	logicalHost := testService + "." + testNS + ".svc.cluster.local:8080"
	logicalURL := "http://" + logicalHost + "/api/items?page=1"

	for i := 0; i < 100; i++ {
		req, err := http.NewRequest(http.MethodGet, logicalURL, nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(body) != logicalHost {
			t.Fatalf("unexpected Host echo %q, want %q", string(body), logicalHost)
		}
	}

	if n := hits1.Load(); n != 50 {
		t.Fatalf("pod-1 got %d requests, want 50", n)
	}
	if n := hits2.Load(); n != 50 {
		t.Fatalf("pod-2 got %d requests, want 50", n)
	}
	if _, ok := pathSeen.Load("/api/items?page=1"); !ok {
		t.Fatal("path/query not preserved at downstream")
	}
}

func TestGateNoEndpoints(t *testing.T) {
	gate := newFakeGate(t, testServiceObject())
	req, _ := http.NewRequest(http.MethodGet, "http://"+testService+".svc.cluster.local/x", nil)
	_, err := gate.RoundTrip(req)
	if !errors.Is(err, ErrNoEndpoints) {
		t.Fatalf("expected ErrNoEndpoints, got %v", err)
	}
}

func TestGateClosedRejectsRequests(t *testing.T) {
	gate := newFakeGate(t, testServiceObject())
	if err := gate.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := gate.Close(); err != nil {
		t.Fatalf("second close should be idempotent: %v", err)
	}

	req, _ := http.NewRequest(http.MethodGet, "http://"+testService+".svc.cluster.local/x", nil)
	if _, err := gate.RoundTrip(req); err == nil {
		t.Fatal("expected error after close")
	}
}

func TestGateRewritePreservesUserHost(t *testing.T) {
	var gotHost string
	var mu sync.Mutex

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotHost = r.Host
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })

	gate := newFakeGate(t,
		serviceWithTargetPort(int32(port)),
		testEndpointSlice("slice-1", []discoveryv1.Endpoint{{Addresses: []string{"127.0.0.1"}, TargetRef: podRef("pod-1", "uid-1")}}, testPorts(int32(port))),
	)

	req, _ := http.NewRequest(http.MethodGet, "http://"+testService+".svc.cluster.local:8080/x", nil)
	req.Host = "custom.example.com:8080" // user explicitly set Host
	client := &http.Client{Transport: gate, Timeout: 5 * time.Second}
	if _, err := client.Do(req); err != nil {
		t.Fatalf("do: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotHost != "custom.example.com:8080" {
		t.Fatalf("user Host should be preserved, got %q", gotHost)
	}
}

func TestHooksReceiveEvents(t *testing.T) {
	var mu sync.Mutex
	var kinds []EventKind
	var picked []string

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })

	hook := HookFunc(func(e Event) {
		mu.Lock()
		defer mu.Unlock()
		kinds = append(kinds, e.Kind)
		if e.Kind == EventEndpointPicked {
			picked = append(picked, e.Endpoint)
		}
	})

	cs := fake.NewSimpleClientset(
		serviceWithTargetPort(int32(port)),
		testEndpointSlice("slice-1", []discoveryv1.Endpoint{{Addresses: []string{"127.0.0.1"}, TargetRef: podRef("pod-1", "uid-1")}}, testPorts(int32(port))),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	gate, err := NewGate(ctx, testNS, testService,
		WithClientset(cs), WithCacheSyncTimeout(5*time.Second), WithHook(hook))
	if err != nil {
		t.Fatalf("NewGate: %v", err)
	}
	defer gate.Close()

	client := &http.Client{Transport: gate, Timeout: 5 * time.Second}
	req, _ := http.NewRequest(http.MethodGet, "http://"+testService+".svc.cluster.local:8080/x", nil)
	if _, err := client.Do(req); err != nil {
		t.Fatalf("do: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(picked) != 1 || picked[0] != "pod-1" {
		t.Fatalf("expected one picked event for pod-1, got %v", picked)
	}
	hasDone := false
	for _, k := range kinds {
		if k == EventRequestDone {
			hasDone = true
		}
	}
	if !hasDone {
		t.Fatalf("expected EventRequestDone, got %v", kinds)
	}
}

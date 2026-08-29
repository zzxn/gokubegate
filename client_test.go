package gokubegate

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestNewClientAndClose(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	host, portStr, err := splitHostPortURL(srv.URL)
	if err != nil {
		t.Fatalf("parse server addr: %v", err)
	}
	port, _ := strconv.Atoi(portStr)

	cs := fake.NewSimpleClientset(
		serviceWithTargetPort(int32(port)),
		testEndpointSlice("slice-1", []discoveryv1.Endpoint{{Addresses: []string{host}, TargetRef: podRef("pod-1", "uid-1")}}, testPorts(int32(port))),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := NewClient(ctx, testNS, testService,
		WithClientset(cs), WithCacheSyncTimeout(5*time.Second), WithDrainTimeout(1*time.Second))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	if client.Mode() != "pod" {
		t.Fatalf("mode should be pod, got %q", client.Mode())
	}
	eps := client.Endpoints()
	if len(eps) != 1 || eps[0].PodName != "pod-1" {
		t.Fatalf("unexpected endpoints: %+v", eps)
	}

	resp, err := client.Get("http://" + testService + "." + testNS + ".svc.cluster.local:8080/health")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()

	if err := client.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second close should be idempotent: %v", err)
	}

	if _, err := client.Get("http://" + testService + ".svc.cluster.local/health"); err == nil {
		t.Fatal("expected error after close")
	}
}

func TestNewClientValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := NewClient(ctx, "", "svc"); err == nil {
		t.Fatal("expected error for empty namespace")
	}
	if _, err := NewClient(ctx, "ns", ""); err == nil {
		t.Fatal("expected error for empty service")
	}
	if _, err := NewClient(ctx, "ns", "svc", WithPort(70000)); err == nil {
		t.Fatal("expected error for invalid port")
	}
	if _, err := NewClient(ctx, "ns", "svc", WithScheme("ftp")); err == nil {
		t.Fatal("expected error for invalid scheme")
	}
}

func TestNewClientInClusterConfigError(t *testing.T) {
	// Outside a cluster and without injected config this must fail fast with
	// a clear error instead of hanging or silently using the wrong transport.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := NewClient(ctx, "ns", "svc", WithPort(8080), WithCacheSyncTimeout(time.Second))
	if err == nil {
		t.Fatal("expected error when no cluster config is available")
	}
	if !strings.Contains(err.Error(), "in-cluster config") {
		t.Fatalf("expected in-cluster config hint, got: %v", err)
	}
}

func splitHostPortURL(rawURL string) (host, port string, err error) {
	trimmed := strings.TrimPrefix(rawURL, "http://")
	return net.SplitHostPort(trimmed)
}

package gokubegate

import (
	"context"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	testNS      = "ns"
	testService = "svc"
	testPort    = 8080
)

func ptr[T any](v T) *T { return &v }

func testPorts(port int32) []discoveryv1.EndpointPort {
	return []discoveryv1.EndpointPort{{Port: &port, Protocol: ptr(corev1.ProtocolTCP)}}
}

func testEndpointSlice(name string, endpoints []discoveryv1.Endpoint, ports []discoveryv1.EndpointPort) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNS,
			Labels:    map[string]string{discoveryv1.LabelServiceName: testService},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints:   endpoints,
		Ports:       ports,
	}
}

func podRef(name, uid string) *corev1.ObjectReference {
	return &corev1.ObjectReference{Kind: "Pod", Name: name, Namespace: testNS, UID: types.UID(uid)}
}

func newTestDiscovery(t *testing.T, objs ...runtime.Object) (*discovery, *Config) {
	t.Helper()
	cs := fake.NewSimpleClientset(objs...)
	cfg := defaultConfig()
	cfg.Namespace = testNS
	cfg.Service = testService
	cfg.Port = testPort
	cfg.Clientset = cs

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d, err := newDiscovery(ctx, cfg)
	if err != nil {
		t.Fatalf("newDiscovery: %v", err)
	}
	t.Cleanup(d.stop)
	return d, cfg
}

func waitForSnapshot(t *testing.T, d *discovery, cond func(*EndpointSnapshot) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond(d.currentSnapshot()) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("snapshot condition not met; ready=%d",
		len(d.currentSnapshot().Backends))
}

func TestReconcileAggregatesReadyEndpoints(t *testing.T) {
	d, _ := newTestDiscovery(t,
		testEndpointSlice("slice-1", []discoveryv1.Endpoint{
			{Addresses: []string{"10.0.0.1"}, Conditions: discoveryv1.EndpointConditions{Ready: ptr(true)}, TargetRef: podRef("pod-1", "uid-1")},
			{Addresses: []string{"10.0.0.2"}, Conditions: discoveryv1.EndpointConditions{Ready: ptr(false)}, TargetRef: podRef("pod-2", "uid-2")},
			{Addresses: []string{"10.0.0.3"}, Conditions: discoveryv1.EndpointConditions{Terminating: ptr(true)}, TargetRef: podRef("pod-3", "uid-3")},
			{Addresses: []string{"10.0.0.4"}, TargetRef: podRef("pod-4", "uid-4")}, // ready nil => usable
		}, testPorts(testPort)),
	)

	waitForSnapshot(t, d, func(s *EndpointSnapshot) bool { return len(s.Backends) == 2 })

	got := map[string]bool{}
	for _, b := range d.currentSnapshot().Backends {
		if b.address == "10.0.0.1:8080" {
			got["pod-1"] = true
		}
		if b.address == "10.0.0.4:8080" {
			got["pod-4"] = true
		}
	}
	if len(got) != 2 {
		t.Fatalf("expected pod-1 and pod-4 only, got %v", got)
	}
}

func TestReconcileMergesMultipleSlices(t *testing.T) {
	d, _ := newTestDiscovery(t,
		testEndpointSlice("slice-1", []discoveryv1.Endpoint{{Addresses: []string{"10.0.0.1"}, TargetRef: podRef("pod-1", "uid-1")}}, testPorts(testPort)),
		testEndpointSlice("slice-2", []discoveryv1.Endpoint{{Addresses: []string{"10.0.0.2"}, TargetRef: podRef("pod-2", "uid-2")}}, testPorts(testPort)),
	)
	waitForSnapshot(t, d, func(s *EndpointSnapshot) bool { return len(s.Backends) == 2 })
}

func TestReconcileIgnoresOtherServicesAndPortMismatch(t *testing.T) {
	other := testEndpointSlice("other", []discoveryv1.Endpoint{{Addresses: []string{"10.0.0.9"}, TargetRef: podRef("pod-9", "uid-9")}}, testPorts(testPort))
	other.Labels[discoveryv1.LabelServiceName] = "other-service"

	wrongPort := testEndpointSlice("wrong-port", []discoveryv1.Endpoint{{Addresses: []string{"10.0.0.8"}, TargetRef: podRef("pod-8", "uid-8")}}, testPorts(9090))

	d, _ := newTestDiscovery(t, other, wrongPort)
	waitForSnapshot(t, d, func(s *EndpointSnapshot) bool { return len(s.Backends) == 0 })
}

func TestReconcileUIDReuseCreatesFreshBackend(t *testing.T) {
	d, _ := newTestDiscovery(t,
		testEndpointSlice("slice-1", []discoveryv1.Endpoint{
			{Addresses: []string{"10.0.0.1"}, TargetRef: podRef("pod-a", "uid-a")},
			{Addresses: []string{"10.0.0.1"}, TargetRef: podRef("pod-b", "uid-b")}, // same IP, new pod
		}, testPorts(testPort)),
	)
	waitForSnapshot(t, d, func(s *EndpointSnapshot) bool { return len(s.Backends) == 2 })
}

func TestScaleDownDrainsRemovedEndpoint(t *testing.T) {
	d, _ := newTestDiscovery(t,
		testEndpointSlice("slice-1", []discoveryv1.Endpoint{
			{Addresses: []string{"10.0.0.1"}, TargetRef: podRef("pod-1", "uid-1")},
			{Addresses: []string{"10.0.0.2"}, TargetRef: podRef("pod-2", "uid-2")},
		}, testPorts(testPort)),
	)
	waitForSnapshot(t, d, func(s *EndpointSnapshot) bool { return len(s.Backends) == 2 })

	// Capture the backend that is about to be removed.
	var removed *PodBackend
	for _, b := range d.currentSnapshot().Backends {
		if b.podName == "pod-2" {
			removed = b
		}
	}
	if removed == nil {
		t.Fatal("pod-2 backend not found before scale-down")
	}

	// Scale down: remove pod-2 from the slice.
	updated := testEndpointSlice("slice-1", []discoveryv1.Endpoint{
		{Addresses: []string{"10.0.0.1"}, TargetRef: podRef("pod-1", "uid-1")},
	}, testPorts(testPort))
	if _, err := d.clientset.DiscoveryV1().EndpointSlices(testNS).Update(context.Background(), updated, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update endpointslice: %v", err)
	}

	waitForSnapshot(t, d, func(s *EndpointSnapshot) bool { return len(s.Backends) == 1 })

	// The removed backend must be marked draining and finish draining
	// (closed channel) without interrupting anything.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if removed.draining.Load() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !removed.draining.Load() {
		t.Fatal("removed backend was never marked draining")
	}
	select {
	case <-removed.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("removed backend did not finish draining")
	}
	if d.currentSnapshot().Backends[0].podName != "pod-1" {
		t.Fatalf("remaining backend should be pod-1, got %q", d.currentSnapshot().Backends[0].podName)
	}
}

func TestEmptyEndpointsAfterStop(t *testing.T) {
	d, _ := newTestDiscovery(t)
	waitForSnapshot(t, d, func(s *EndpointSnapshot) bool { return len(s.Backends) == 0 })
	d.stop()
	if !d.currentSnapshot().isEmpty() {
		t.Fatal("snapshot should be empty after stop")
	}
}

func TestConcurrentStopIsIdempotent(t *testing.T) {
	d, _ := newTestDiscovery(t)

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.stop()
		}()
	}
	wg.Wait()

	select {
	case <-d.stopCh:
	default:
		t.Fatal("stop channel should be closed")
	}
	if !d.currentSnapshot().isEmpty() {
		t.Fatal("snapshot should be empty after concurrent stop")
	}
}

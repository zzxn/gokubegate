package gokubegate

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/informers"
	discoveryinformers "k8s.io/client-go/informers/discovery/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

// discovery watches the EndpointSlices of one service and publishes an
// immutable EndpointSnapshot. The request path only reads the atomic snapshot.
type discovery struct {
	cfg *Config

	clientset kubernetes.Interface
	factory   informers.SharedInformerFactory
	informer  discoveryinformers.EndpointSliceInformer

	trigger  chan struct{}
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	snapshot atomic.Pointer[EndpointSnapshot]
	version  uint64

	// mu guards backends/draining; touched only by the reconcile worker
	// and Close (via finishDrain).
	mu       sync.Mutex
	backends map[EndpointKey]*PodBackend
	draining map[*PodBackend]struct{}
}

type endpointMeta struct {
	address  string // host:port
	podName  string
	nodeName string
}

// newDiscovery builds the informer, resolves the target port, and blocks until
// the informer cache has synced (or CacheSyncTimeout elapses).
func newDiscovery(ctx context.Context, cfg *Config) (*discovery, error) {
	clientset, err := resolveClientset(cfg)
	if err != nil {
		return nil, err
	}

	// Resolve the target port from the Service unless explicitly configured.
	port := cfg.Port
	if port == 0 {
		resolved, err := resolveServicePort(ctx, clientset, cfg)
		if err != nil {
			return nil, err
		}
		port = resolved
	}
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("gokubegate: invalid resolved port %d for %s/%s", port, cfg.Namespace, cfg.Service)
	}
	cfg.Port = port
	cfg.resolveLogging()

	d := &discovery{
		cfg:       cfg,
		clientset: clientset,
		trigger:   make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
		backends:  map[EndpointKey]*PodBackend{},
		draining:  map[*PodBackend]struct{}{},
	}

	selector := labels.Set{discoveryv1.LabelServiceName: cfg.Service}.AsSelector().String()
	factory := informers.NewSharedInformerFactoryWithOptions(clientset, 0,
		informers.WithNamespace(cfg.Namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = selector
		}),
	)
	informer := factory.Discovery().V1().EndpointSlices()
	if _, err := informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { d.triggerReconcile() },
		UpdateFunc: func(any, any) { d.triggerReconcile() },
		DeleteFunc: func(any) { d.triggerReconcile() },
	}); err != nil {
		return nil, fmt.Errorf("gokubegate: register endpointslice handler: %w", err)
	}

	d.factory = factory
	d.informer = informer

	factory.Start(d.stopCh)

	syncCtx, cancel := context.WithTimeout(ctx, cfg.CacheSyncTimeout)
	defer cancel()
	if !cache.WaitForCacheSync(syncCtx.Done(), informer.Informer().HasSynced) {
		close(d.stopCh)
		return nil, fmt.Errorf("gokubegate: endpointslice informer cache sync timed out for %s/%s", cfg.Namespace, cfg.Service)
	}

	// Build the initial snapshot synchronously so that NewClient/NewGate
	// return with a ready-to-use endpoint set.
	d.reconcile()

	d.wg.Go(d.run)

	return d, nil
}

func resolveClientset(cfg *Config) (kubernetes.Interface, error) {
	if cfg.Clientset != nil {
		return cfg.Clientset, nil
	}
	restCfg, err := resolveRESTConfig(cfg)
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("gokubegate: create kubernetes clientset: %w", err)
	}
	return clientset, nil
}

func resolveRESTConfig(cfg *Config) (*rest.Config, error) {
	if cfg.RESTConfig != nil {
		return cfg.RESTConfig, nil
	}
	if cfg.KubeConfig != "" {
		rc, err := clientcmd.BuildConfigFromFlags("", cfg.KubeConfig)
		if err != nil {
			return nil, fmt.Errorf("gokubegate: load kubeconfig %q: %w", cfg.KubeConfig, err)
		}
		return rc, nil
	}
	rc, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("gokubegate: in-cluster config unavailable (use WithRESTConfig/WithKubeConfig/WithClientset when running outside a cluster): %w", err)
	}
	return rc, nil
}

// resolveServicePort resolves the pod target port from the Service object.
// It prefers the numeric TargetPort; named target ports cannot be resolved
// without pod metadata and are rejected with guidance.
func resolveServicePort(ctx context.Context, clientset kubernetes.Interface, cfg *Config) (int32, error) {
	svc, err := clientset.CoreV1().Services(cfg.Namespace).Get(ctx, cfg.Service, metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("gokubegate: resolve port for %s/%s: %w", cfg.Namespace, cfg.Service, err)
	}
	var namedTargetPort string
	for _, p := range svc.Spec.Ports {
		if p.Protocol != "" && p.Protocol != corev1.ProtocolTCP {
			continue
		}
		if cfg.PortName != "" && p.Name != cfg.PortName {
			continue
		}
		if port, ok := numericTargetPort(p); ok {
			return port, nil
		}
		if namedTargetPort == "" {
			namedTargetPort = p.TargetPort.StrVal
		}
	}
	if namedTargetPort != "" {
		return 0, fmt.Errorf("gokubegate: service %s/%s uses named targetPort %q which cannot be resolved without pod metadata; set WithPort or use a numeric targetPort", cfg.Namespace, cfg.Service, namedTargetPort)
	}
	if cfg.PortName != "" {
		return 0, fmt.Errorf("gokubegate: service %s/%s has no TCP port named %q", cfg.Namespace, cfg.Service, cfg.PortName)
	}
	return 0, fmt.Errorf("gokubegate: service %s/%s has no TCP ports", cfg.Namespace, cfg.Service)
}

func numericTargetPort(p corev1.ServicePort) (int32, bool) {
	switch p.TargetPort.Type {
	case intstr.Int:
		return int32(p.TargetPort.IntVal), true
	case intstr.String:
		return 0, false
	default:
		// TargetPort unset: defaults to the service port.
		return p.Port, true
	}
}

func (d *discovery) triggerReconcile() {
	select {
	case d.trigger <- struct{}{}:
	default:
	}
}

func (d *discovery) run() {
	for {
		select {
		case <-d.trigger:
			d.reconcile()
		case <-d.stopCh:
			return
		}
	}
}

func (d *discovery) reconcile() {
	candidates, err := d.listCandidates()
	if err != nil {
		d.cfg.Logger.Warn("gokubegate: list endpoint slices failed",
			"service", d.cfg.Service, "error", err)
		emit(d.cfg, Event{Kind: EventReconcile, Result: "error", Err: err})
		return
	}

	ready, draining := d.applyCandidates(candidates)
	emit(d.cfg, Event{Kind: EventEndpointsUpdated, Ready: ready, Draining: draining})
	emit(d.cfg, Event{Kind: EventReconcile, Result: "success"})
	d.cfg.Logger.Debug("gokubegate: endpoints reconciled",
		"service", d.cfg.Service, "ready", ready, "draining", draining)
}

func (d *discovery) listCandidates() (map[EndpointKey]endpointMeta, error) {
	slices, err := d.informer.Lister().EndpointSlices(d.cfg.Namespace).List(
		labels.SelectorFromSet(labels.Set{discoveryv1.LabelServiceName: d.cfg.Service}))
	if err != nil {
		return nil, err
	}

	candidates := make(map[EndpointKey]endpointMeta)
	for _, s := range slices {
		for _, ep := range s.Endpoints {
			if !endpointUsable(ep) {
				continue
			}
			port, ok := matchPort(s.Ports, d.cfg.Port)
			if !ok {
				continue
			}
			for _, addr := range ep.Addresses {
				if net.ParseIP(addr) == nil {
					// FQDN addresses are ignored in v0.1.
					continue
				}
				uid := ""
				podName := ""
				if ep.TargetRef != nil {
					uid = string(ep.TargetRef.UID)
					podName = ep.TargetRef.Name
				}
				nodeName := ""
				if ep.NodeName != nil {
					nodeName = *ep.NodeName
				}
				key := EndpointKey{
					Namespace: d.cfg.Namespace,
					Service:   d.cfg.Service,
					UID:       uid,
					Address:   addr,
					Port:      port,
				}
				candidates[key] = endpointMeta{
					address:  addressHostPort(addr, port),
					podName:  podName,
					nodeName: nodeName,
				}
			}
		}
	}
	return candidates, nil
}

func (d *discovery) applyCandidates(candidates map[EndpointKey]endpointMeta) (ready, draining int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	keys := make([]EndpointKey, 0, len(candidates))
	for k := range candidates {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })

	next := make([]*PodBackend, 0, len(keys))
	newBackends := make(map[EndpointKey]*PodBackend, len(keys))
	for _, k := range keys {
		b, ok := d.backends[k]
		if !ok {
			meta := candidates[k]
			b = newPodBackend(k, meta.address, meta.podName, meta.nodeName, d.logicalHost(), d.cfg)
			d.cfg.Logger.Debug("gokubegate: discovered endpoint",
				"service", d.cfg.Service, "address", meta.address, "pod", meta.podName)
		}
		newBackends[k] = b
		next = append(next, b)
	}

	removals := make([]*PodBackend, 0)
	for k, b := range d.backends {
		if _, ok := newBackends[k]; !ok {
			removals = append(removals, b)
		}
	}
	d.backends = newBackends

	d.version++
	d.snapshot.Store(&EndpointSnapshot{Version: d.version, Backends: next, Updated: time.Now()})
	for _, removed := range removals {
		d.draining[removed] = struct{}{}
		d.cfg.Logger.Debug("gokubegate: endpoint removed, draining",
			"service", d.cfg.Service, "address", removed.address, "pod", removed.podName)
		removed.startDrain(d.cfg, func() { d.finishDrain(removed) })
	}

	return len(next), len(d.draining)
}

func (d *discovery) finishDrain(backend *PodBackend) {
	d.mu.Lock()
	delete(d.draining, backend)
	d.mu.Unlock()
}

func (d *discovery) currentSnapshot() *EndpointSnapshot {
	s := d.snapshot.Load()
	if s == nil {
		return &EndpointSnapshot{}
	}
	return s
}

func (d *discovery) logicalHost() string {
	return logicalServiceHost(d.cfg)
}

func (d *discovery) stop() {
	d.stopOnce.Do(func() {
		close(d.stopCh)
		if d.factory != nil {
			d.factory.Shutdown()
		}
		d.wg.Wait()

		d.mu.Lock()
		defer d.mu.Unlock()
		for _, b := range d.backends {
			d.draining[b] = struct{}{}
			b.startDrain(d.cfg, func() { d.finishDrain(b) })
		}
		d.backends = map[EndpointKey]*PodBackend{}
		d.snapshot.Store(&EndpointSnapshot{})
	})
}

// endpointUsable reports whether an endpoint may receive new requests:
// ready != false AND terminating != true (nil follows Kubernetes semantics).
func endpointUsable(ep discoveryv1.Endpoint) bool {
	if ep.Conditions.Ready != nil && !*ep.Conditions.Ready {
		return false
	}
	if ep.Conditions.Terminating != nil && *ep.Conditions.Terminating {
		return false
	}
	return true
}

// matchPort finds the TCP port matching the configured target port.
func matchPort(ports []discoveryv1.EndpointPort, want int32) (int32, bool) {
	for _, p := range ports {
		if p.Port == nil || *p.Port != want {
			continue
		}
		if p.Protocol != nil && *p.Protocol != corev1.ProtocolTCP {
			continue
		}
		return *p.Port, true
	}
	return 0, false
}

func portString(port int32) string {
	return fmt.Sprintf("%d", port)
}

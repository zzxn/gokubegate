package harness

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// kindConfig describes a 2-node kind cluster (control-plane + worker).
const kindConfig = `kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
`

// EnsureCluster reuses an existing kind cluster with the same name or creates
// one, then loads the kubeconfig and waits for nodes to be ready.
func (h *Harness) EnsureCluster(ctx context.Context) error {
	if err := os.MkdirAll(h.cacheDir(), 0o755); err != nil {
		return err
	}
	out, err := h.run(ctx, h.opts.KindPath, "get", "clusters")
	if err != nil {
		return fmt.Errorf("kind get clusters: %w", err)
	}
	found := false
	for _, name := range strings.Fields(out) {
		if name == h.opts.ClusterName {
			found = true
			break
		}
	}
	if found {
		fmt.Printf("[e2e] reusing kind cluster %q\n", h.opts.ClusterName)
		kubeconfig, err := h.run(ctx, h.opts.KindPath, "get", "kubeconfig", "--name", h.opts.ClusterName)
		if err != nil {
			return fmt.Errorf("kind get kubeconfig: %w", err)
		}
		if os.Getenv("GOKUBEGATE_E2E_KUBECONFIG") == "" {
			if err := os.WriteFile(h.kubeconfigPath(), []byte(kubeconfig), 0o600); err != nil {
				return fmt.Errorf("write e2e kubeconfig: %w", err)
			}
		}
	} else {
		fmt.Printf("[e2e] creating kind cluster %q ...\n", h.opts.ClusterName)
		cfg := filepath.Join(h.cacheDir(), "kind-config.yaml")
		if err := os.WriteFile(cfg, []byte(kindConfig), 0o644); err != nil {
			return err
		}
		if _, err := h.run(ctx, h.opts.KindPath, "create", "cluster",
			"--name", h.opts.ClusterName,
			"--kubeconfig", h.kubeconfigPath(),
			"--config", cfg,
		); err != nil {
			return fmt.Errorf("kind create cluster: %w", err)
		}
	}

	rc, err := clientcmd.BuildConfigFromFlags("", h.kubeconfigPath())
	if err != nil {
		return fmt.Errorf("load e2e kubeconfig: %w", err)
	}
	kube, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return fmt.Errorf("create clientset: %w", err)
	}
	if h.kube == nil {
		h.kube = kube
	}

	return WaitFor(ctx, time.Second, 2*time.Minute, "nodes ready", func() (bool, error) {
		nodes, err := h.kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		ready := 0
		for _, n := range nodes.Items {
			for _, c := range n.Status.Conditions {
				if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
					ready++
				}
			}
		}
		return ready >= 2, nil
	})
}

// DeleteCluster removes the kind cluster and the local e2e cache.
func (h *Harness) DeleteCluster(ctx context.Context) {
	if h.opts.KeepCluster || h.externalCluster {
		if h.externalCluster {
			fmt.Println("[e2e] preserving explicitly supplied cluster")
			return
		}
		fmt.Println("[e2e] GOKUBEGATE_E2E_KEEP=1: keeping cluster")
		return
	}
	_, _ = h.run(ctx, h.opts.KindPath, "delete", "cluster", "--name", h.opts.ClusterName)
	_ = os.RemoveAll(h.cacheDir())
}

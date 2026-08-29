// Package harness drives the gokubegate e2e tests against a real Kubernetes
// cluster running in Docker via kind. All artifacts (cluster, images, temp
// kubeconfig) are confined to the repo's .cache/ directory and the Docker
// daemon; the host environment is not modified.
package harness

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
)

// Options configures a Harness.
type Options struct {
	ClusterName string // kind cluster name
	Namespace   string
	Service     string
	ImagePrefix string // image name prefix, e.g. gokubegate-e2e
	// KindPath and KubectlPath default to "kind"/"kubectl" from PATH, falling
	// back to .cache/kind/kind inside the repo.
	KindPath    string
	KubectlPath string
	KeepCluster bool
	// RepoRoot defaults to the repo root (three levels up from this package).
	RepoRoot string
}

// Harness owns the cluster lifecycle and k8s helpers.
type Harness struct {
	opts            Options
	kubect          string // resolved kubectl binary
	kube            kubernetes.Interface
	externalCluster bool // explicitly supplied clusters are never deleted
}

// New creates a Harness with defaults resolved.
func New(opts Options) *Harness {
	if opts.ClusterName == "" {
		if external := os.Getenv("GOKUBEGATE_E2E_CLUSTER"); external != "" {
			opts.ClusterName = external
		} else {
			opts.ClusterName = "gokubegate-e2e"
		}
	}
	if opts.Namespace == "" {
		opts.Namespace = "gokubegate-e2e"
	}
	if opts.Service == "" {
		opts.Service = "downstream"
	}
	if opts.ImagePrefix == "" {
		opts.ImagePrefix = "gokubegate-e2e"
	}
	if opts.RepoRoot == "" {
		opts.RepoRoot = repoRoot()
	}
	if opts.KindPath == "" {
		opts.KindPath = resolveKindPath(opts.RepoRoot)
	}
	h := &Harness{opts: opts, externalCluster: os.Getenv("GOKUBEGATE_E2E_CLUSTER") != ""}
	if opts.KubectlPath != "" {
		h.kubect = opts.KubectlPath
	} else {
		h.kubect = "kubectl"
	}
	return h
}

// repoRoot resolves the repository root from the package directory
// (test/e2e/harness -> repo root).
func repoRoot() string {
	abs, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		return "."
	}
	return abs
}

func resolveKindPath(repo string) string {
	if p := os.Getenv("GOKUBEGATE_E2E_KIND"); p != "" {
		return p
	}
	cached := filepath.Join(repo, ".cache", "kind", "kind")
	if info, err := os.Stat(cached); err == nil && !info.IsDir() {
		return cached
	}
	return "kind" // fall back to PATH
}

func (h *Harness) repo(path string) string { return filepath.Join(h.opts.RepoRoot, path) }

func (h *Harness) cacheDir() string {
	return h.repo(filepath.Join(".cache", "e2e"))
}

func (h *Harness) kubeconfigPath() string {
	if p := os.Getenv("GOKUBEGATE_E2E_KUBECONFIG"); p != "" {
		return p
	}
	return h.repo(filepath.Join(".cache", "e2e", "kubeconfig"))
}

func (h *Harness) run(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%s %v: %w: %s", name, args, err, out)
	}
	return string(out), nil
}

func (h *Harness) runIn(ctx context.Context, dir, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("(dir %s) %s %v: %w: %s", dir, name, args, err, out)
	}
	return string(out), nil
}

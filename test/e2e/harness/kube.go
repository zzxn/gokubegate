package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TesterResult mirrors the JSON emitted by the tester CLI.
type TesterResult struct {
	Phase       string         `json:"phase"`
	Requests    int            `json:"requests"`
	Success     int            `json:"success"`
	Errors      int            `json:"errors"`
	ByEndpoint  map[string]int `json:"byEndpoint"`
	Reused      int            `json:"reused"`
	Connections int            `json:"connections"`
	ReusedRatio float64        `json:"reusedRatio"`
	HostEcho    string         `json:"hostEcho"`
	PathEcho    string         `json:"pathEcho"`
	QueryEcho   string         `json:"queryEcho"`
	DurationMs  int64          `json:"durationMs"`
}

// ParseTesterResult decodes the tester CLI's single JSON line from its logs.
func ParseTesterResult(logs string) (TesterResult, error) {
	var r TesterResult
	if err := json.Unmarshal([]byte(strings.TrimSpace(logs)), &r); err != nil {
		return r, fmt.Errorf("parse tester result: %w (logs: %q)", err, logs)
	}
	return r, nil
}

// Apply runs kubectl apply -f on a manifest (with --server-side=false).
func (h *Harness) Apply(ctx context.Context, path string) error {
	if _, err := h.run(ctx, h.kubect, "--kubeconfig", h.kubeconfigPath(), "apply", "-f", path); err != nil {
		return fmt.Errorf("apply %s: %w", path, err)
	}
	return nil
}

// DeleteNamespace removes the e2e namespace (best-effort).
func (h *Harness) DeleteNamespace(ctx context.Context) {
	_, _ = h.run(ctx, h.kubect, "--kubeconfig", h.kubeconfigPath(), "delete", "namespace", h.opts.Namespace, "--ignore-not-found", "--wait=false")
}

// BuildAndLoadImages builds the downstream and tester binaries on the host,
// packages them into scratch images, and loads them into the kind cluster.
func (h *Harness) BuildAndLoadImages(ctx context.Context) error {
	builds := []struct {
		dir string
		bin string
		img string
	}{
		{dir: "test/e2e/downstream", bin: "downstream", img: h.opts.ImagePrefix + "/downstream:test"},
		{dir: "test/e2e/tester", bin: "tester", img: h.opts.ImagePrefix + "/tester:test"},
	}
	for _, b := range builds {
		binary := h.repo(filepath.Join(b.dir, b.bin))
		cmd := exec.CommandContext(ctx, "go", "build", "-o", binary, "./"+b.dir)
		cmd.Dir = h.opts.RepoRoot
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH=amd64")
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("build %s: %w: %s", b.bin, err, out)
		}
		if _, err := h.runIn(ctx, h.opts.RepoRoot, "docker", "build",
			"-t", b.img,
			"-f", h.repo(filepath.Join(b.dir, "Dockerfile")),
			".",
		); err != nil {
			return fmt.Errorf("docker build %s: %w", b.img, err)
		}
		if _, err := h.run(ctx, h.opts.KindPath, "load", "docker-image", b.img, "--name", h.opts.ClusterName); err != nil {
			return fmt.Errorf("kind load %s: %w", b.img, err)
		}
	}
	return nil
}

// WaitDeploymentReady waits until the deployment has want ready replicas.
func (h *Harness) WaitDeploymentReady(ctx context.Context, name string, want int32) error {
	return WaitFor(ctx, 500*time.Millisecond, 3*time.Minute, fmt.Sprintf("deployment %s ready (%d replicas)", name, want), func() (bool, error) {
		dep, err := h.kube.AppsV1().Deployments(h.opts.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return dep.Generation <= dep.Status.ObservedGeneration &&
			dep.Spec.Replicas != nil && *dep.Spec.Replicas == want &&
			dep.Status.ReadyReplicas == want && dep.Status.UnavailableReplicas == 0, nil
	})
}

// WaitEndpointsReady waits until the Service has want ready (non-terminating)
// endpoints in EndpointSlice.
func (h *Harness) WaitEndpointsReady(ctx context.Context, want int) error {
	return WaitFor(ctx, 500*time.Millisecond, 3*time.Minute, fmt.Sprintf("endpoints ready (%d)", want), func() (bool, error) {
		slices, err := h.kube.DiscoveryV1().EndpointSlices(h.opts.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: "kubernetes.io/service-name=" + h.opts.Service,
		})
		if err != nil {
			return false, err
		}
		count := 0
		for _, s := range slices.Items {
			for _, ep := range s.Endpoints {
				if ep.Conditions.Ready != nil && !*ep.Conditions.Ready {
					continue
				}
				if ep.Conditions.Terminating != nil && *ep.Conditions.Terminating {
					continue
				}
				count++
			}
		}
		return count == want, nil
	})
}

// ScaleDeployment sets the deployment replica count.
func (h *Harness) ScaleDeployment(ctx context.Context, name string, replicas int32) error {
	scale, err := h.kube.AppsV1().Deployments(h.opts.Namespace).GetScale(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get scale %s: %w", name, err)
	}
	scale.Spec = autoscalingv1.ScaleSpec{Replicas: replicas}
	if _, err := h.kube.AppsV1().Deployments(h.opts.Namespace).UpdateScale(ctx, name, scale, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("scale %s: %w", name, err)
	}
	return nil
}

// PodNames returns pod names matching a label selector.
func (h *Harness) PodNames(ctx context.Context, labelSelector string) ([]string, error) {
	pods, err := h.kube.CoreV1().Pods(h.opts.Namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return nil, err
	}
	var names []string
	for _, p := range pods.Items {
		names = append(names, p.Name)
	}
	sort.Strings(names)
	return names, nil
}

// ReadyEndpointPodNames returns the pod names currently eligible for new
// traffic according to the Service's EndpointSlices.
func (h *Harness) ReadyEndpointPodNames(ctx context.Context) ([]string, error) {
	slices, err := h.kube.DiscoveryV1().EndpointSlices(h.opts.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "kubernetes.io/service-name=" + h.opts.Service,
	})
	if err != nil {
		return nil, err
	}
	var names []string
	for _, slice := range slices.Items {
		for _, ep := range slice.Endpoints {
			if ep.Conditions.Ready != nil && !*ep.Conditions.Ready {
				continue
			}
			if ep.Conditions.Terminating != nil && *ep.Conditions.Terminating {
				continue
			}
			if ep.TargetRef != nil && ep.TargetRef.Name != "" {
				names = append(names, ep.TargetRef.Name)
			}
		}
	}
	sort.Strings(names)
	return names, nil
}

// Exec runs a command inside a pod via kubectl exec.
func (h *Harness) Exec(ctx context.Context, pod string, args ...string) (string, error) {
	full := append([]string{"--kubeconfig", h.kubeconfigPath(), "exec", pod, "-n", h.opts.Namespace, "--"}, args...)
	return h.run(ctx, h.kubect, full...)
}

// PodStats reads the request counter from one downstream pod.
func (h *Harness) PodStats(ctx context.Context, pod string) (int64, error) {
	out, err := h.Exec(ctx, pod, "/app/downstream", "-stats")
	if err != nil {
		return 0, err
	}
	var stats struct {
		Count int64 `json:"count"`
	}
	if err := json.Unmarshal([]byte(out), &stats); err != nil {
		return 0, fmt.Errorf("parse stats for %s: %w (%s)", pod, err, out)
	}
	return stats.Count, nil
}

// SetPodReady changes the readiness state exposed by a downstream pod.
func (h *Harness) SetPodReady(ctx context.Context, pod string, ready bool) error {
	if _, err := h.Exec(ctx, pod, "/app/downstream", "-set-ready="+strconv.FormatBool(ready)); err != nil {
		return fmt.Errorf("set pod %s ready=%t: %w", pod, ready, err)
	}
	return nil
}

// RunTesterJob creates a tester Job, waits for completion, and returns its
// stdout logs. A non-zero exit or job failure is returned as an error.
func (h *Harness) RunTesterJob(ctx context.Context, name string, args []string) (string, error) {
	return h.runTesterJob(ctx, name, "gokubegate-tester", args, true)
}

// RunTesterJobExpectFailure runs a tester under the given ServiceAccount and
// returns its logs once the Job reaches the failed state.
func (h *Harness) RunTesterJobExpectFailure(ctx context.Context, name, serviceAccount string, args []string) (string, error) {
	return h.runTesterJob(ctx, name, serviceAccount, args, false)
}

func (h *Harness) runTesterJob(ctx context.Context, name, serviceAccount string, args []string, wantSuccess bool) (string, error) {
	ns := h.opts.Namespace
	if err := h.deleteJob(ctx, name); err != nil {
		return "", err
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: batchv1.JobSpec{
			BackoffLimit: ptrInt32(0),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					ServiceAccountName: serviceAccount,
					RestartPolicy:      corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:  "tester",
						Image: h.opts.ImagePrefix + "/tester:test",
						Args:  args,
					}},
				},
			},
		},
	}
	if _, err := h.kube.BatchV1().Jobs(ns).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		return "", fmt.Errorf("create job %s: %w", name, err)
	}

	var succeeded bool
	if err := WaitFor(ctx, time.Second, 3*time.Minute, "tester job "+name, func() (bool, error) {
		j, err := h.kube.BatchV1().Jobs(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if j.Status.Succeeded >= 1 {
			succeeded = true
			return true, nil
		}
		if j.Status.Failed > 0 {
			return true, nil
		}
		return false, nil
	}); err != nil {
		return "", err
	}
	logs, err := h.JobLogs(ctx, name)
	if err != nil {
		return "", err
	}
	if succeeded != wantSuccess {
		want := "fail"
		if wantSuccess {
			want = "succeed"
		}
		return logs, fmt.Errorf("job %s unexpectedly did not %s; logs: %s", name, want, logs)
	}
	return logs, nil
}

func (h *Harness) deleteJob(ctx context.Context, name string) error {
	err := h.kube.BatchV1().Jobs(h.opts.Namespace).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: ptrDelete()})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete previous job %s: %w", name, err)
	}
	return WaitFor(ctx, 200*time.Millisecond, 30*time.Second, "delete previous job "+name, func() (bool, error) {
		_, err := h.kube.BatchV1().Jobs(h.opts.Namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	})
}

// JobLogs returns the concatenated logs of a job's pods.
func (h *Harness) JobLogs(ctx context.Context, name string) (string, error) {
	pods, err := h.kube.CoreV1().Pods(h.opts.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + name})
	if err != nil {
		return "", err
	}
	var logs string
	for _, p := range pods.Items {
		req := h.kube.CoreV1().Pods(h.opts.Namespace).GetLogs(p.Name, &corev1.PodLogOptions{})
		data, err := req.DoRaw(ctx)
		if err != nil {
			return "", fmt.Errorf("logs for %s: %w", p.Name, err)
		}
		logs += string(data) + "\n"
	}
	return logs, nil
}

func ptrInt32(v int32) *int32 { return &v }

func ptrDelete() *metav1.DeletionPropagation {
	p := metav1.DeletePropagationForeground
	return &p
}

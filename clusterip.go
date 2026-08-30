package gokubegate

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newClusterIPTransport(ctx context.Context, cfg *Config) (*http.Transport, error) {
	if cfg.Port == 0 {
		clientset, err := resolveClientset(cfg)
		if err != nil {
			return nil, err
		}
		port, err := resolveClusterIPServicePort(ctx, clientset.CoreV1().Services(cfg.Namespace), cfg)
		if err != nil {
			return nil, err
		}
		cfg.Port = port
	}
	return newHTTPTransport(cfg, logicalServiceHost(cfg), cfg.MaxIdleConnsPerPod), nil
}

type serviceGetter interface {
	Get(context.Context, string, metav1.GetOptions) (*corev1.Service, error)
}

func resolveClusterIPServicePort(ctx context.Context, services serviceGetter, cfg *Config) (int32, error) {
	svc, err := services.Get(ctx, cfg.Service, metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("gokubegate: resolve ClusterIP port for %s/%s: %w", cfg.Namespace, cfg.Service, err)
	}
	for _, port := range svc.Spec.Ports {
		if port.Protocol != "" && port.Protocol != corev1.ProtocolTCP {
			continue
		}
		if cfg.PortName != "" && port.Name != cfg.PortName {
			continue
		}
		return port.Port, nil
	}
	if cfg.PortName != "" {
		return 0, fmt.Errorf("gokubegate: service %s/%s has no TCP port named %q", cfg.Namespace, cfg.Service, cfg.PortName)
	}
	return 0, fmt.Errorf("gokubegate: service %s/%s has no TCP ports", cfg.Namespace, cfg.Service)
}

func logicalServiceHost(cfg *Config) string {
	return net.JoinHostPort(
		cfg.Service+"."+cfg.Namespace+".svc."+cfg.ClusterDomain,
		portString(cfg.Port),
	)
}

func shouldCloseClusterIPConnection(denominator uint64) bool {
	return denominator > 0 && rand.Uint64N(denominator) == 0
}

func isEventStreamRequest(req *http.Request) bool {
	for _, value := range req.Header.Values("Accept") {
		for _, mediaType := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(strings.SplitN(mediaType, ";", 2)[0]), "text/event-stream") {
				return true
			}
		}
	}
	return false
}

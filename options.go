package gokubegate

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Mode selects how requests reach the target Kubernetes Service.
type Mode string

const (
	// ModePod discovers ready EndpointSlices and balances each request directly
	// across Pod IPs using isolated per-Pod connection pools.
	ModePod Mode = "pod"
	// ModeClusterIP uses one shared connection pool through the Kubernetes
	// Service ClusterIP. Optional sampled connection closing encourages L4
	// endpoint rotation without guaranteeing request-level balance.
	ModeClusterIP Mode = "clusterip"
)

const minimumConnectionCloseSampleDenominator uint64 = 100

// Config holds the resolved gokubegate configuration. Prefer building it with
// NewClient/NewGate and functional options instead of constructing it directly.
type Config struct {
	Namespace string
	Service   string
	Mode      Mode
	Port      int32
	PortName  string
	Scheme    string
	// ClusterDomain is appended to <service>.<namespace>.svc. to form the
	// logical Host header; defaults to "cluster.local".
	ClusterDomain string

	Strategy         Strategy
	CacheSyncTimeout time.Duration
	DrainTimeout     time.Duration

	MaxIdleConnsPerPod    int
	IdleConnTimeout       time.Duration
	DialTimeout           time.Duration
	TCPKeepAlive          time.Duration
	ResponseHeaderTimeout time.Duration
	// ConnectionCloseSampleDenominator applies only to ModeClusterIP. Zero
	// disables sampled rotation; N closes approximately one in N requests.
	ConnectionCloseSampleDenominator uint64

	// RESTConfig overrides in-cluster config discovery.
	RESTConfig *rest.Config
	// KubeConfig loads a kubeconfig file (out-of-cluster / testing).
	KubeConfig string
	// Clientset injects an existing clientset (advanced / testing). When set,
	// RESTConfig and KubeConfig are ignored.
	Clientset kubernetes.Interface

	// Hooks receive runtime events for observability. Default: none.
	Hooks []Hook
	// Logger for internal diagnostics. Default: discards everything.
	Logger *slog.Logger
	// Debug enables internal debug logging to stderr when no Logger is set.
	Debug bool
}

// Option customizes a Config. Apply via NewClient/NewGate.
type Option func(*Config)

func defaultConfig() *Config {
	return &Config{
		Scheme:                           "http",
		Mode:                             ModePod,
		ClusterDomain:                    "cluster.local",
		CacheSyncTimeout:                 20 * time.Second,
		DrainTimeout:                     30 * time.Second,
		MaxIdleConnsPerPod:               32,
		IdleConnTimeout:                  90 * time.Second,
		DialTimeout:                      5 * time.Second,
		TCPKeepAlive:                     30 * time.Second,
		ResponseHeaderTimeout:            15 * time.Second,
		ConnectionCloseSampleDenominator: 1000,
	}
}

// WithMode selects pod-level or ClusterIP routing. Default: ModePod.
func WithMode(mode Mode) Option {
	return func(c *Config) { c.Mode = mode }
}

// WithConnectionCloseSampleDenominator configures sampled connection
// rotation in ModeClusterIP. Zero disables it; non-zero values must be at
// least 100. Default: 1000 (approximately 0.1% of ordinary HTTP requests).
func WithConnectionCloseSampleDenominator(denominator uint64) Option {
	return func(c *Config) { c.ConnectionCloseSampleDenominator = denominator }
}

// WithPort sets the target TCP port explicitly, skipping Service lookup.
func WithPort(port int32) Option {
	return func(c *Config) { c.Port = port }
}

// WithPortName resolves the target port from the Service by port name.
func WithPortName(name string) Option {
	return func(c *Config) { c.PortName = name }
}

// WithScheme sets the downstream scheme (http or https). Default: http.
func WithScheme(scheme string) Option {
	return func(c *Config) { c.Scheme = scheme }
}

// WithClusterDomain sets the cluster DNS domain. Default: cluster.local.
func WithClusterDomain(domain string) Option {
	return func(c *Config) { c.ClusterDomain = domain }
}

// WithStrategy sets the endpoint selection strategy. Default: round-robin.
func WithStrategy(s Strategy) Option {
	return func(c *Config) { c.Strategy = s }
}

// WithCacheSyncTimeout sets how long ModePod waits for informer cache sync
// during startup. It is unused by ModeClusterIP. Default: 20s.
func WithCacheSyncTimeout(d time.Duration) Option {
	return func(c *Config) { c.CacheSyncTimeout = d }
}

// WithDrainTimeout sets how long a removed endpoint may keep in-flight
// requests before the drain is considered timed out. Default: 30s.
func WithDrainTimeout(d time.Duration) Option {
	return func(c *Config) { c.DrainTimeout = d }
}

// WithMaxIdleConnsPerPod sets the idle keep-alive connection budget per Pod.
// In ModeClusterIP it controls the single shared Service-host pool. Default: 16.
func WithMaxIdleConnsPerPod(n int) Option {
	return func(c *Config) { c.MaxIdleConnsPerPod = n }
}

// WithIdleConnTimeout sets how long idle keep-alive connections are retained.
// Default: 90s.
func WithIdleConnTimeout(d time.Duration) Option {
	return func(c *Config) { c.IdleConnTimeout = d }
}

// WithDialTimeout sets the TCP dial timeout. Default: 5s.
func WithDialTimeout(d time.Duration) Option {
	return func(c *Config) { c.DialTimeout = d }
}

// WithTCPKeepAlive sets the TCP keep-alive period. Default: 30s.
func WithTCPKeepAlive(d time.Duration) Option {
	return func(c *Config) { c.TCPKeepAlive = d }
}

// WithResponseHeaderTimeout sets the timeout for the downstream response
// header. Default: 15s.
func WithResponseHeaderTimeout(d time.Duration) Option {
	return func(c *Config) { c.ResponseHeaderTimeout = d }
}

// WithRESTConfig injects a Kubernetes REST config (out-of-cluster / testing).
func WithRESTConfig(cfg *rest.Config) Option {
	return func(c *Config) { c.RESTConfig = cfg }
}

// WithKubeConfig loads a kubeconfig file (out-of-cluster / testing).
func WithKubeConfig(path string) Option {
	return func(c *Config) { c.KubeConfig = path }
}

// WithClientset injects an existing Kubernetes clientset. Useful when the
// application already owns one, and required for tests with a fake clientset.
func WithClientset(cs kubernetes.Interface) Option {
	return func(c *Config) { c.Clientset = cs }
}

// WithHook registers an event hook. Multiple hooks are allowed and run in
// registration order. Default: none.
func WithHook(h Hook) Option {
	return func(c *Config) { c.Hooks = append(c.Hooks, h) }
}

// WithHooks registers several event hooks at once.
func WithHooks(hooks ...Hook) Option {
	return func(c *Config) { c.Hooks = append(c.Hooks, hooks...) }
}

// WithLogger injects a logger for internal diagnostics. Default: no output.
func WithLogger(l *slog.Logger) Option {
	return func(c *Config) { c.Logger = l }
}

// WithDebug enables internal debug logging to stderr when no logger was set.
func WithDebug() Option {
	return func(c *Config) { c.Debug = true }
}

func (c *Config) validate() error {
	if c.Namespace == "" {
		return fmt.Errorf("gokubegate: namespace is required")
	}
	if c.Service == "" {
		return fmt.Errorf("gokubegate: service is required")
	}
	if c.Mode != ModePod && c.Mode != ModeClusterIP {
		return fmt.Errorf("gokubegate: unsupported mode %q", c.Mode)
	}
	if denominator := c.ConnectionCloseSampleDenominator; c.Mode == ModeClusterIP && denominator != 0 && denominator < minimumConnectionCloseSampleDenominator {
		return fmt.Errorf("gokubegate: connection close sample denominator must be 0 or at least %d", minimumConnectionCloseSampleDenominator)
	}
	if c.Port < 0 || c.Port > 65535 {
		return fmt.Errorf("gokubegate: invalid port %d", c.Port)
	}
	if c.PortName != "" && c.Port != 0 {
		return fmt.Errorf("gokubegate: WithPort and WithPortName are mutually exclusive")
	}
	if c.Scheme != "http" && c.Scheme != "https" {
		return fmt.Errorf("gokubegate: unsupported scheme %q", c.Scheme)
	}
	if c.ClusterDomain == "" {
		return fmt.Errorf("gokubegate: cluster domain must not be empty")
	}
	if c.CacheSyncTimeout <= 0 {
		return fmt.Errorf("gokubegate: cache sync timeout must be positive")
	}
	if c.DrainTimeout <= 0 {
		return fmt.Errorf("gokubegate: drain timeout must be positive")
	}
	if c.MaxIdleConnsPerPod <= 0 {
		return fmt.Errorf("gokubegate: max idle conns per pod must be positive")
	}
	if c.IdleConnTimeout <= 0 || c.DialTimeout <= 0 || c.TCPKeepAlive <= 0 || c.ResponseHeaderTimeout <= 0 {
		return fmt.Errorf("gokubegate: transport timeouts must be positive")
	}
	return nil
}

func (c *Config) resolveLogging() {
	if c.Debug && c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
}

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zzxn/gokubegate"
)

const readyMarker = "GOKUBEGATE_TESTER_READY"

type TesterResult struct {
	Phase           string            `json:"phase"`
	Mode            string            `json:"mode"`
	Requests        int               `json:"requests"`
	Success         int               `json:"success"`
	Errors          int               `json:"errors"`
	Concurrency     int               `json:"concurrency"`
	ByEndpoint      map[string]int    `json:"byEndpoint"`
	ByPod           map[string]int    `json:"byPod"`
	ErrorKinds      map[string]int    `json:"errorKinds,omitempty"`
	ErrorSamples    map[string]string `json:"errorSamples,omitempty"`
	Reused          int               `json:"reused"`
	Connections     int               `json:"connections"`
	Rotations       int               `json:"rotations"`
	ReusedRatio     float64           `json:"reusedRatio"`
	HostEcho        string            `json:"hostEcho"`
	PathEcho        string            `json:"pathEcho"`
	QueryEcho       string            `json:"queryEcho"`
	DurationMs      int64             `json:"durationMs"`
	Throughput      float64           `json:"throughput"`
	LatencyP50Ms    float64           `json:"latencyP50Ms"`
	LatencyP95Ms    float64           `json:"latencyP95Ms"`
	LatencyP99Ms    float64           `json:"latencyP99Ms"`
	LatencyMaxMs    float64           `json:"latencyMaxMs"`
	Windows         []WindowResult    `json:"windows,omitempty"`
	EndpointUpdates []EndpointUpdate  `json:"endpointUpdates,omitempty"`
}

// WindowResult summarizes requests completed during one second of sustained
// load, so a short outage cannot be hidden by aggregate success counts.
type WindowResult struct {
	Second       int            `json:"second"`
	Requests     int            `json:"requests"`
	Success      int            `json:"success"`
	Errors       int            `json:"errors"`
	LatencyP95Ms float64        `json:"latencyP95Ms"`
	ByPod        map[string]int `json:"byPod,omitempty"`
}

type EndpointUpdate struct {
	ElapsedMs int64 `json:"elapsedMs"`
	Ready     int   `json:"ready"`
	Draining  int   `json:"draining"`
}

type identity struct {
	Pod   string `json:"pod"`
	Host  string `json:"host"`
	Path  string `json:"path"`
	Query string `json:"query"`
}

type windowMetrics struct {
	requests int
	success  int
	errors   int
	latency  []time.Duration
	byPod    map[string]int
}

type metrics struct {
	mu sync.Mutex

	start        time.Time
	success      int
	errors       int
	byEndpoint   map[string]int
	byPod        map[string]int
	errorKinds   map[string]int
	errorSamples map[string]string
	reused       int
	connections  int
	rotations    int
	latency      []time.Duration
	windows      map[int]*windowMetrics
	updates      []EndpointUpdate
	last         identity
}

func newMetrics(start time.Time) *metrics {
	return &metrics{
		start:        start,
		byEndpoint:   make(map[string]int),
		byPod:        make(map[string]int),
		errorKinds:   make(map[string]int),
		errorSamples: make(map[string]string),
		windows:      make(map[int]*windowMetrics),
	}
}

func (m *metrics) handleEvent(event gokubegate.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch event.Kind {
	case gokubegate.EventEndpointsUpdated:
		m.updates = append(m.updates, EndpointUpdate{
			ElapsedMs: time.Since(m.start).Milliseconds(),
			Ready:     event.Ready,
			Draining:  event.Draining,
		})
	case gokubegate.EventEndpointPicked:
		m.byEndpoint[event.Endpoint]++
	case gokubegate.EventConnectionRotated:
		m.rotations++
	case gokubegate.EventRequestDone:
		m.connections++
		if event.Reused {
			m.reused++
		}
	}
}

func (m *metrics) begin(start time.Time, initialReady int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.start = start
	m.updates = []EndpointUpdate{{Ready: initialReady}}
}

func (m *metrics) record(latency time.Duration, err error, id identity) {
	m.mu.Lock()
	defer m.mu.Unlock()

	second := int(time.Since(m.start) / time.Second)
	window := m.windows[second]
	if window == nil {
		window = &windowMetrics{byPod: make(map[string]int)}
		m.windows[second] = window
	}
	window.requests++
	window.latency = append(window.latency, latency)
	m.latency = append(m.latency, latency)

	if err != nil {
		m.errors++
		window.errors++
		kind := classifyError(err)
		m.errorKinds[kind]++
		if _, exists := m.errorSamples[kind]; !exists {
			sample := err.Error()
			if len(sample) > 300 {
				sample = sample[:300]
			}
			m.errorSamples[kind] = sample
		}
		return
	}
	m.success++
	window.success++
	if id.Host != "" {
		m.last = id
	}
	if id.Pod != "" {
		m.byPod[id.Pod]++
		window.byPod[id.Pod]++
	}
}

func (m *metrics) result(phase, mode string, concurrency int, elapsed time.Duration) TesterResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := TesterResult{
		Phase:           phase,
		Mode:            mode,
		Requests:        m.success + m.errors,
		Success:         m.success,
		Errors:          m.errors,
		Concurrency:     concurrency,
		ByEndpoint:      m.byEndpoint,
		ByPod:           m.byPod,
		ErrorKinds:      m.errorKinds,
		ErrorSamples:    m.errorSamples,
		Reused:          m.reused,
		Connections:     m.connections,
		Rotations:       m.rotations,
		HostEcho:        m.last.Host,
		PathEcho:        m.last.Path,
		QueryEcho:       m.last.Query,
		DurationMs:      elapsed.Milliseconds(),
		LatencyP50Ms:    percentileMs(m.latency, 0.50),
		LatencyP95Ms:    percentileMs(m.latency, 0.95),
		LatencyP99Ms:    percentileMs(m.latency, 0.99),
		LatencyMaxMs:    percentileMs(m.latency, 1),
		EndpointUpdates: append([]EndpointUpdate(nil), m.updates...),
	}
	if elapsed > 0 {
		result.Throughput = float64(result.Success) / elapsed.Seconds()
	}
	if m.connections > 0 {
		result.ReusedRatio = float64(m.reused) / float64(m.connections)
	}

	seconds := make([]int, 0, len(m.windows))
	for second := range m.windows {
		seconds = append(seconds, second)
	}
	sort.Ints(seconds)
	for _, second := range seconds {
		window := m.windows[second]
		result.Windows = append(result.Windows, WindowResult{
			Second:       second,
			Requests:     window.requests,
			Success:      window.success,
			Errors:       window.errors,
			LatencyP95Ms: percentileMs(window.latency, 0.95),
			ByPod:        window.byPod,
		})
	}
	return result
}

func percentileMs(values []time.Duration, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	index := int(float64(len(ordered)-1) * quantile)
	return float64(ordered[index].Microseconds()) / 1000
}

func classifyError(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "context_canceled"
	}
	message := strings.ToLower(err.Error())
	for _, match := range []struct {
		contains string
		kind     string
	}{
		{"no ready endpoints", "no_endpoints"},
		{"connection refused", "connection_refused"},
		{"connection reset", "connection_reset"},
		{"timeout", "timeout"},
		{"unexpected eof", "unexpected_eof"},
		{"eof", "eof"},
	} {
		if strings.Contains(message, match.contains) {
			return match.kind
		}
	}
	return "other"
}

func main() {
	phase := flag.String("phase", "run", "phase label for output")
	namespace := flag.String("namespace", "", "k8s namespace of the target service")
	service := flag.String("service", "", "k8s service name")
	targetURL := flag.String("url", "", "request URL")
	requests := flag.Int("requests", 100, "number of requests when duration is zero")
	concurrency := flag.Int("concurrency", 1, "number of concurrent request workers")
	duration := flag.Duration("duration", 0, "sustained load duration; overrides requests when non-zero")
	rate := flag.Int("rate", 0, "optional total requests/second cap; 0 = unlimited (requires -duration)")
	host := flag.String("host", "", "optional explicit Host header")
	mode := flag.String("mode", "http", "request mode: http or sse")
	gateMode := flag.String("gate-mode", string(gokubegate.ModePod), "gokubegate mode: pod or clusterip")
	closeDenominator := flag.Uint64("connection-close-denominator", 1000, "ClusterIP connection-close sample denominator; 0 disables")
	maxIdle := flag.Int("max-idle-conns", 32, "max idle keep-alive connections per Pod")
	flag.Parse()

	if *namespace == "" || *service == "" || *targetURL == "" {
		fmt.Fprintln(os.Stderr, "namespace, service and url are required")
		os.Exit(2)
	}
	if *concurrency < 1 || (*duration <= 0 && *requests < 1) {
		fmt.Fprintln(os.Stderr, "concurrency and requests must be positive")
		os.Exit(2)
	}
	if *rate < 0 || (*rate > 0 && *duration <= 0) {
		fmt.Fprintln(os.Stderr, "rate requires a positive duration")
		os.Exit(2)
	}
	if *mode != "http" && *mode != "sse" {
		fmt.Fprintln(os.Stderr, "mode must be http or sse")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	metrics := newMetrics(time.Now())
	client, err := gokubegate.NewClient(ctx, *namespace, *service,
		gokubegate.WithMode(gokubegate.Mode(*gateMode)),
		gokubegate.WithConnectionCloseSampleDenominator(*closeDenominator),
		gokubegate.WithMaxIdleConnsPerPod(*maxIdle),
		gokubegate.WithHook(gokubegate.HookFunc(metrics.handleEvent)),
		gokubegate.WithCacheSyncTimeout(30*time.Second),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewClient: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	// The harness waits for this marker before lifecycle mutations, ensuring
	// one long-lived client observes every EndpointSlice transition.
	start := time.Now()
	metrics.begin(start, len(client.Endpoints()))
	fmt.Println(readyMarker)

	var next atomic.Int64
	deadline := start.Add(*duration)
	paceInterval := time.Duration(0)
	if *rate > 0 {
		paceInterval = time.Duration(float64(time.Second) * float64(*concurrency) / float64(*rate))
	}
	var workers sync.WaitGroup
	for range *concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			requestIndex := 0
			workerStart := time.Now()
			for {
				if *duration > 0 {
					if !time.Now().Before(deadline) {
						return
					}
				} else if next.Add(1) > int64(*requests) {
					return
				}
				if paceInterval > 0 {
					target := workerStart.Add(time.Duration(requestIndex) * paceInterval)
					if wait := time.Until(target); wait > 0 {
						timer := time.NewTimer(wait)
						select {
						case <-timer.C:
						case <-ctx.Done():
							timer.Stop()
							return
						}
					}
					requestIndex++
				}
				if *mode == "sse" {
					doSSE(ctx, client, metrics, *targetURL, *host)
				} else {
					doRequest(ctx, client, metrics, *targetURL, *host)
				}
			}
		}()
	}
	workers.Wait()

	result := metrics.result(*phase, *gateMode, *concurrency, time.Since(start))
	out, _ := json.Marshal(result)
	fmt.Println(string(out))
}

func doSSE(ctx context.Context, client *gokubegate.Client, metrics *metrics, targetURL, host string) {
	started := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		metrics.record(time.Since(started), err, identity{})
		return
	}
	if host != "" {
		req.Host = host
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := client.Do(req)
	if err != nil {
		metrics.record(time.Since(started), err, identity{})
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		metrics.record(time.Since(started), errors.New("http_status_"+strconv.Itoa(resp.StatusCode)), identity{})
		return
	}

	var selectedPod string
	events := 0
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event struct {
			Pod string `json:"pod"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			metrics.record(time.Since(started), fmt.Errorf("decode_sse: %w", err), identity{})
			return
		}
		if selectedPod == "" {
			selectedPod = event.Pod
		} else if selectedPod != event.Pod {
			metrics.record(time.Since(started), fmt.Errorf("sse_pod_changed:%s_to_%s", selectedPod, event.Pod), identity{})
			return
		}
		events++
	}
	if err := scanner.Err(); err != nil {
		metrics.record(time.Since(started), err, identity{})
		return
	}
	if events == 0 || selectedPod == "" {
		metrics.record(time.Since(started), errors.New("sse_no_events"), identity{})
		return
	}
	metrics.record(time.Since(started), nil, identity{Pod: selectedPod})
}

func doRequest(ctx context.Context, client *gokubegate.Client, metrics *metrics, targetURL, host string) {
	started := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		metrics.record(time.Since(started), err, identity{})
		return
	}
	if host != "" {
		req.Host = host
	}

	resp, err := client.Do(req)
	if err != nil {
		metrics.record(time.Since(started), err, identity{})
		return
	}
	body, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil {
		metrics.record(time.Since(started), readErr, identity{})
		return
	}
	if closeErr != nil {
		metrics.record(time.Since(started), closeErr, identity{})
		return
	}
	if resp.StatusCode != http.StatusOK {
		metrics.record(time.Since(started), errors.New("http_status_"+strconv.Itoa(resp.StatusCode)), identity{})
		return
	}

	var id identity
	if err := json.Unmarshal(body, &id); err != nil {
		metrics.record(time.Since(started), fmt.Errorf("decode_response: %w", err), identity{})
		return
	}
	metrics.record(time.Since(started), nil, id)
}

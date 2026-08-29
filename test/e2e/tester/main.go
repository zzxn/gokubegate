package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/zzxn/gokubegate"
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

func main() {
	phase := flag.String("phase", "run", "phase label for output")
	namespace := flag.String("namespace", "", "k8s namespace of the target service")
	service := flag.String("service", "", "k8s service name")
	url := flag.String("url", "", "request URL")
	requests := flag.Int("requests", 100, "number of requests")
	host := flag.String("host", "", "optional explicit Host header")
	flag.Parse()

	if *namespace == "" || *service == "" || *url == "" {
		fmt.Fprintln(os.Stderr, "namespace, service and url are required")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var mu sync.Mutex
	byEndpoint := map[string]int{}
	var reused, connections int

	hook := gokubegate.HookFunc(func(e gokubegate.Event) {
		mu.Lock()
		defer mu.Unlock()
		switch e.Kind {
		case gokubegate.EventEndpointPicked:
			byEndpoint[e.Endpoint]++
		case gokubegate.EventRequestDone:
			connections++
			if e.Reused {
				reused++
			}
		}
	})

	client, err := gokubegate.NewClient(ctx, *namespace, *service,
		gokubegate.WithHook(hook),
		gokubegate.WithCacheSyncTimeout(30*time.Second),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewClient: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	res := TesterResult{Phase: *phase, Requests: *requests}
	start := time.Now()

	for i := 0; i < *requests; i++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, *url, nil)
		if err != nil {
			res.Errors++
			continue
		}
		if *host != "" {
			req.Host = *host
		}
		resp, err := client.Do(req)
		if err != nil {
			res.Errors++
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			res.Errors++
			continue
		}
		res.Success++
		var id struct {
			Host  string `json:"host"`
			Path  string `json:"path"`
			Query string `json:"query"`
		}
		if err := json.Unmarshal(body, &id); err == nil && id.Host != "" {
			res.HostEcho = id.Host
			res.PathEcho = id.Path
			res.QueryEcho = id.Query
		}
	}
	res.DurationMs = time.Since(start).Milliseconds()

	mu.Lock()
	res.ByEndpoint = byEndpoint
	mu.Unlock()
	if connections > 0 {
		res.ReusedRatio = float64(reused) / float64(connections)
	}

	out, _ := json.Marshal(res)
	fmt.Println(string(out))
}

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
)

var (
	hits  atomic.Int64
	ready atomic.Bool
)

// identity is what the downstream pod returns for every request, so tests can
// verify Host/path/query preservation end to end.
type identity struct {
	Pod    string `json:"pod"`
	Node   string `json:"node"`
	Host   string `json:"host"`
	Path   string `json:"path"`
	Query  string `json:"query"`
	Remote string `json:"remote"`
}

func main() {
	statsMode := flag.Bool("stats", false, "query http://127.0.0.1:8080/stats and print")
	setReady := flag.String("set-ready", "", "set readiness through the local admin endpoint (true or false)")
	flag.Parse()

	if *statsMode {
		resp, err := http.Get("http://127.0.0.1:8080/stats")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(os.Stdout, resp.Body)
		return
	}
	if *setReady != "" {
		value, err := strconv.ParseBool(*setReady)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid -set-ready value: %v\n", err)
			os.Exit(2)
		}
		body := fmt.Sprintf(`{"ready":%t}`, value)
		resp, err := http.Post("http://127.0.0.1:8080/admin/ready", "application/json", strings.NewReader(body))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(os.Stdout, resp.Body)
		if resp.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		return
	}

	ready.Store(true)
	http.HandleFunc("/", handleRoot)
	http.HandleFunc("/ready", handleReady)
	http.HandleFunc("/stats", handleStats)
	http.HandleFunc("/admin/ready", handleAdminReady)

	addr := ":8080"
	if a := os.Getenv("DOWNSTREAM_ADDR"); a != "" {
		addr = a
	}
	if err := http.ListenAndServe(addr, nil); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func podName() string {
	if p := os.Getenv("POD_NAME"); p != "" {
		return p
	}
	h, _ := os.Hostname()
	return h
}

func nodeName() string { return os.Getenv("NODE_NAME") }

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	hits.Add(1)
	writeJSON(w, identity{
		Pod:    podName(),
		Node:   nodeName(),
		Host:   r.Host,
		Path:   r.URL.Path,
		Query:  r.URL.RawQuery,
		Remote: r.RemoteAddr,
	})
}

func handleStats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{"pod": podName(), "count": hits.Load()})
}

func handleReady(w http.ResponseWriter, _ *http.Request) {
	if ready.Load() {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, "ok")
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = fmt.Fprintln(w, "not ready")
}

// handleAdminReady toggles readiness for scale-down/NotReady scenarios.
func handleAdminReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Ready bool `json:"ready"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ready.Store(body.Ready)
	writeJSON(w, map[string]any{"ready": body.Ready})
}

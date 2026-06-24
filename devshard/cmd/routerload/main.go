// routerload — a real-traffic load generator for the Gonka router, built to
// replicate the "big customer" methodology from the rate-limit investigation
// and to add the one metric they could not see from outside: in-flight
// concurrency.
//
// It is an OPEN-LOOP generator: it fires requests at a fixed *arrival rate*
// ("request starts/sec") and does NOT wait for one to finish before starting
// the next. This is the only way to reproduce in-flight accumulation
// (in-flight = arrival_rate × latency). Closed-loop tools cannot show the
// concurrent-request cliff the customer hit.
//
// It replicates the customer's run shape:
//   - real long-context payload (configurable max_tokens / prompt size)
//   - unique, non-cacheable prompt per request (UUID + random padding)
//   - no retries
//   - 90s client timeout
//   - RPS ramp (e.g. 5,10,15,20,25), fixed duration per step
//   - records 200 / 429 / timeout / other, latency p50/p95/p99/max,
//     the exact first-429 body, and MAX IN-FLIGHT per step
//   - writes a summary CSV (and optional per-request CSV for upstream trace
//     lookup, via an injected request id in the OpenAI `user` field).
//
// AUTH: Bearer API key read from env ROUTER_API_KEY. Never hardcoded, never
// printed, never written to any output file.
//
// 1 vs 2 ESCROW: escrow count is a server-side router setting, not a client
// knob. Run this once per configuration and tag each run with -label
// (e.g. -label 1escrow then -label 2escrow), then diff the two summary CSVs.
//
// USAGE
//   export ROUTER_API_KEY=sk-...
//   go run ./cmd/routerload \
//     -url https://api.gonkascan.com/v1/chat/completions \
//     -model MiniMaxAI/MiniMax-M2.7 \
//     -rps 5,10,15,20,25 -dur 60s \
//     -max-tokens 4096 -prompt-tokens 2000 \
//     -timeout 90s -label 1escrow
//
// COST WARNING: this sends real, billable inference requests against real
// hardware. At -rps 25 -dur 60s the last step alone launches ~1500 requests.
// Start small, scale up deliberately. Ctrl-C stops cleanly and prints a
// partial summary.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	var (
		url          = flag.String("url", env("ROUTER_URL", "https://api.gonkascan.com/v1/chat/completions"), "chat-completions endpoint")
		model        = flag.String("model", env("ROUTER_MODEL", "MiniMaxAI/MiniMax-M2.7"), "model id")
		rpsList      = flag.String("rps", "5,10,15,20,25", "comma-separated RPS ramp steps (request starts/sec)")
		dur          = flag.Duration("dur", 60*time.Second, "duration per RPS step")
		maxTokens    = flag.Int("max-tokens", 4096, "max_tokens (long-context => bigger => longer in-flight hold)")
		promptTokens = flag.Int("prompt-tokens", 2000, "approx input prompt size in tokens (controls payload weight)")
		timeout      = flag.Duration("timeout", 90*time.Second, "per-request client timeout")
		stream       = flag.Bool("stream", false, "use streaming responses")
		label        = flag.String("label", "run", "tag for this run (use 1escrow / 2escrow to compare)")
		outPath      = flag.String("out", "", "summary CSV path (default routerload-<label>.csv)")
		detailPath   = flag.String("detail", "", "optional per-request CSV (request_id, status, latency_ms, ts) for upstream trace lookup")
		warmup       = flag.Bool("warmup", true, "send one probe request first to validate auth/reachability")
	)
	flag.Parse()

	apiKey := os.Getenv("ROUTER_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "ERROR: set ROUTER_API_KEY env var (Bearer token). It is never printed or written to files.")
		os.Exit(2)
	}
	steps := parseInts(*rpsList)
	if len(steps) == 0 {
		fmt.Fprintln(os.Stderr, "ERROR: -rps must list at least one positive integer")
		os.Exit(2)
	}
	if *outPath == "" {
		*outPath = "routerload-" + *label + ".csv"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// One shared client. High connection ceilings so the *client* is never the
	// bottleneck — we want to measure the server, not local socket limits.
	client := &http.Client{
		Timeout: *timeout,
		Transport: &http.Transport{
			MaxIdleConns:        0,    // unlimited
			MaxConnsPerHost:     0,    // unlimited
			MaxIdleConnsPerHost: 4096, // keep-alive reuse
			IdleConnTimeout:     90 * time.Second,
			ForceAttemptHTTP2:   false, // prefer many HTTP/1.1 conns for true concurrency
		},
	}

	var totalPlanned int
	for _, r := range steps {
		totalPlanned += r * int(dur.Seconds())
	}
	fmt.Printf("=== routerload (%s) ===\n", *label)
	fmt.Printf("target:   %s\n", *url)
	fmt.Printf("model:    %s\n", *model)
	fmt.Printf("ramp:     rps=%v  dur/step=%v  => ~%d total requests\n", steps, *dur, totalPlanned)
	fmt.Printf("payload:  prompt~%d tok, max_tokens=%d, stream=%v, timeout=%v\n", *promptTokens, *maxTokens, *stream, *timeout)
	fmt.Printf("auth:     Bearer ROUTER_API_KEY (len=%d, not shown)\n\n", len(apiKey))

	if *warmup {
		if err := probe(ctx, client, *url, apiKey, *model); err != nil {
			fmt.Fprintf(os.Stderr, "WARMUP FAILED: %v\n", err)
			fmt.Fprintln(os.Stderr, "Aborting before ramp. Check URL / ROUTER_API_KEY / model.")
			os.Exit(1)
		}
		fmt.Print("warmup: ok (auth + reachability confirmed)\n\n")
	}

	summary, err := os.Create(*outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: create %s: %v\n", *outPath, err)
		os.Exit(1)
	}
	defer summary.Close()
	fmt.Fprintln(summary, "label,rps,sent,http_200,http_429,timeout,other,p50_ms,p95_ms,p99_ms,max_ms,max_inflight,first_429_offset_s")

	var detail *os.File
	if *detailPath != "" {
		detail, err = os.Create(*detailPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: create %s: %v\n", *detailPath, err)
			os.Exit(1)
		}
		defer detail.Close()
		fmt.Fprintln(detail, "request_id,rps_step,status,class,latency_ms,start_unix_ms")
	}

	fmt.Printf("%-6s %-7s %-7s %-7s %-9s %-7s %-9s %-9s %-9s %-9s %-12s %-12s\n",
		"rps", "sent", "200", "429", "timeout", "other", "p50ms", "p95ms", "p99ms", "maxms", "max_inflt", "first429@s")
	fmt.Println(strings.Repeat("-", 120))

	gen := &payloadGen{model: *model, maxTokens: *maxTokens, promptTokens: *promptTokens, stream: *stream}

	for _, rps := range steps {
		if ctx.Err() != nil {
			break
		}
		st := runStep(ctx, client, *url, apiKey, gen, rps, *dur, detail)
		writeSummary(summary, *label, rps, st)
		printStep(rps, st)
	}

	fmt.Printf("\nsummary written: %s\n", *outPath)
	if detail != nil {
		fmt.Printf("per-request CSV: %s\n", *detailPath)
	}
	fmt.Println("\nTo compare 1 vs 2 escrows: re-run with the router switched to the other")
	fmt.Println("escrow config and a different -label, then diff the two summary CSVs.")
}

// ---- stats per step ----

type stepStats struct {
	sent        int64
	http200     int64
	http429     int64
	timeouts    int64
	other       int64
	maxInflight int64
	first429    time.Duration // offset from step start; <0 = none
	first429Set int32
	latsMu      sync.Mutex
	lats        []time.Duration
	body429Once sync.Once
	body429     string
}

func runStep(ctx context.Context, client *http.Client, url, apiKey string, gen *payloadGen, rps int, dur time.Duration, detail *os.File) *stepStats {
	st := &stepStats{first429: -1}
	var inflight int64
	var wg sync.WaitGroup

	interval := time.Second / time.Duration(rps)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	stepStart := time.Now()
	deadline := stepStart.Add(dur)

	var detailMu sync.Mutex

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case now := <-ticker.C:
			if now.After(deadline) {
				break loop
			}
			atomic.AddInt64(&st.sent, 1)
			cur := atomic.AddInt64(&inflight, 1)
			updateMax(&st.maxInflight, cur)
			wg.Add(1)
			go func(reqStart time.Time) {
				defer wg.Done()
				defer atomic.AddInt64(&inflight, -1)
				rid := newID()
				status, class, lat, body := doRequest(ctx, client, url, apiKey, gen, rid)
				st.record(class, status, lat, body, reqStart.Sub(stepStart))
				if detail != nil {
					detailMu.Lock()
					fmt.Fprintf(detail, "%s,%d,%d,%s,%d,%d\n", rid, rps, status, class, lat.Milliseconds(), reqStart.UnixMilli())
					detailMu.Unlock()
				}
			}(now)
		}
	}

	// Drain outstanding requests (bounded by the client timeout).
	wg.Wait()
	return st
}

func (st *stepStats) record(class string, status int, lat time.Duration, body string, offset time.Duration) {
	switch class {
	case "ok":
		atomic.AddInt64(&st.http200, 1)
	case "429":
		atomic.AddInt64(&st.http429, 1)
		if atomic.CompareAndSwapInt32(&st.first429Set, 0, 1) {
			st.first429 = offset
		}
		st.body429Once.Do(func() { st.body429 = strings.TrimSpace(body) })
	case "timeout":
		atomic.AddInt64(&st.timeouts, 1)
	default:
		atomic.AddInt64(&st.other, 1)
	}
	if lat > 0 {
		st.latsMu.Lock()
		st.lats = append(st.lats, lat)
		st.latsMu.Unlock()
	}
}

// ---- HTTP ----

func doRequest(ctx context.Context, client *http.Client, url, apiKey string, gen *payloadGen, rid string) (status int, class string, lat time.Duration, body string) {
	payload := gen.build(rid)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return 0, "other", 0, err.Error()
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("X-Request-Id", rid)

	start := time.Now()
	resp, err := client.Do(req)
	lat = time.Since(start)
	if err != nil {
		if ctx.Err() != nil {
			return 0, "other", lat, "context cancelled"
		}
		// http.Client.Timeout and context deadline both surface here.
		if isTimeout(err) {
			return 0, "timeout", lat, err.Error()
		}
		return 0, "other", lat, err.Error()
	}
	defer resp.Body.Close()
	// Read the body fully (capped) so keep-alive can reuse the connection,
	// and so we can capture the 429 error body verbatim.
	const capBody = 512
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	io.Copy(io.Discard, resp.Body)
	snippet := string(rb)
	if len(snippet) > capBody {
		snippet = snippet[:capBody]
	}

	switch {
	case resp.StatusCode == 200:
		return resp.StatusCode, "ok", lat, ""
	case resp.StatusCode == 429:
		return resp.StatusCode, "429", lat, snippet
	default:
		return resp.StatusCode, "other", lat, snippet
	}
}

func probe(ctx context.Context, client *http.Client, url, apiKey, model string) error {
	body, _ := json.Marshal(map[string]any{
		"model":      model,
		"messages":   []map[string]string{{"role": "user", "content": "ping"}},
		"max_tokens": 1,
		"stream":     false,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return fmt.Errorf("auth rejected (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(rb)))
	}
	if resp.StatusCode >= 500 {
		return fmt.Errorf("upstream unhealthy (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(rb)))
	}
	return nil
}

func isTimeout(err error) bool {
	type timeouter interface{ Timeout() bool }
	if te, ok := err.(timeouter); ok && te.Timeout() {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "Client.Timeout") || strings.Contains(s, "deadline exceeded") || strings.Contains(s, "timeout")
}

// ---- payload ----

type payloadGen struct {
	model        string
	maxTokens    int
	promptTokens int
	stream       bool
}

// build returns a unique, non-cacheable OpenAI chat-completions body. The
// request id is embedded both in the content and the `user` field so upstream
// traces can be looked up by it (mirrors the customer's request_user_id trick).
func (g *payloadGen) build(rid string) []byte {
	// ~4 chars/token rough heuristic. Padding is unique per request.
	approxChars := g.promptTokens * 4
	content := "req=" + rid + " " + uniqueFiller(approxChars)
	m := map[string]any{
		"model":      g.model,
		"messages":   []map[string]string{{"role": "user", "content": content}},
		"max_tokens": g.maxTokens,
		"stream":     g.stream,
		"user":       rid,
	}
	b, _ := json.Marshal(m)
	return b
}

// uniqueFiller builds ~n chars of pseudo-text that varies per call so the
// router/model never serves a cached completion.
func uniqueFiller(n int) string {
	if n < 16 {
		n = 16
	}
	const words = "alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike november oscar papa quebec romeo sierra tango uniform victor whiskey xray yankee zulu "
	var b strings.Builder
	b.Grow(n + 24)
	// seed with randomness so two requests of identical size still differ
	seed := make([]byte, 8)
	rand.Read(seed)
	b.WriteString(hex.EncodeToString(seed))
	b.WriteByte(' ')
	for b.Len() < n {
		b.WriteString(words)
	}
	return b.String()[:n]
}

func newID() string {
	b := make([]byte, 12)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// ---- output helpers ----

func writeSummary(f *os.File, label string, rps int, st *stepStats) {
	p50, p95, p99, max := percentiles(st.lats)
	first := ""
	if st.first429 >= 0 {
		first = strconv.FormatFloat(st.first429.Seconds(), 'f', 1, 64)
	}
	fmt.Fprintf(f, "%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s\n",
		label, rps, st.sent, st.http200, st.http429, st.timeouts, st.other,
		p50.Milliseconds(), p95.Milliseconds(), p99.Milliseconds(), max.Milliseconds(),
		st.maxInflight, first)
	// Stash the first 429 body next to the CSV the first time we see one.
	if st.body429 != "" {
		fmt.Fprintf(os.Stderr, "  [rps=%d] first 429 body: %s\n", rps, st.body429)
	}
}

func printStep(rps int, st *stepStats) {
	p50, p95, p99, max := percentiles(st.lats)
	first := "-"
	if st.first429 >= 0 {
		first = fmt.Sprintf("%.1f", st.first429.Seconds())
	}
	fmt.Printf("%-6d %-7d %-7d %-7d %-9d %-7d %-9d %-9d %-9d %-9d %-12d %-12s\n",
		rps, st.sent, st.http200, st.http429, st.timeouts, st.other,
		p50.Milliseconds(), p95.Milliseconds(), p99.Milliseconds(), max.Milliseconds(),
		st.maxInflight, first)
}

func percentiles(lats []time.Duration) (p50, p95, p99, max time.Duration) {
	if len(lats) == 0 {
		return
	}
	s := make([]time.Duration, len(lats))
	copy(s, lats)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	at := func(p float64) time.Duration {
		i := int(float64(len(s)) * p)
		if i >= len(s) {
			i = len(s) - 1
		}
		return s[i]
	}
	return at(0.50), at(0.95), at(0.99), s[len(s)-1]
}

func updateMax(addr *int64, v int64) {
	for {
		old := atomic.LoadInt64(addr)
		if v <= old || atomic.CompareAndSwapInt64(addr, old, v) {
			return
		}
	}
}

func parseInts(csv string) []int {
	var out []int
	for _, p := range strings.Split(csv, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if n, err := strconv.Atoi(p); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	return out
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

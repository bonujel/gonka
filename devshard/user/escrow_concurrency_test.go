//go:build escrowbench

// escrow_concurrency_test.go — a measurement harness (NOT a pass/fail test) that
// reproduces the "big customer" scenario described in the rate-limit
// investigation: long-context, high-concurrency inference traffic driven against
// ONE escrow vs TWO escrows, to locate the bottleneck in the devshard
// coordination layer and quantify how much a second escrow buys.
//
// WHAT THIS MEASURES
//   - The devshard COORDINATION layer only: nonce chaining, per-host state
//     machine critical sections (host.mu), diff propagation, signature
//     bookkeeping (session.mu) — driven by concurrent user-side goroutines,
//     exactly as concurrent HTTP requests would drive a production session.
//   - The inference engine is SIMULATED with a fixed per-request delay
//     (latencyEngine) standing in for "long context => slow response". This
//     isolates coordination cost from real model speed.
//
// WHAT THIS DOES *NOT* MEASURE (important honesty caveats)
//   - Real GPU / vLLM capacity, KV-cache/memory pressure, real token speed.
//   - Real host-to-host network latency (clients are in-process).
//   - The new-api gateway (api.gonkascan.com) layer, where the customer's
//     actual `too many concurrent requests` 429 string is produced.
//   - On-chain escrow creation / settlement.
//   In reality two escrows may be sampled onto the *same* physical GPUs, in
//   which case the shared GPU layer (not modeled here) becomes the real ceiling.
//   This harness answers one precise question: does the devshard coordination
//   layer itself serialize a single escrow, and does a second escrow scale it?
//
// RUN
//   cd devshard
//   go test -tags escrowbench -run TestEscrowConcurrency -v -timeout 30m ./user/
//
// TUNE (env vars, all optional)
//   DEVSHARD_BENCH_DELAY_MS  simulated per-inference latency (default 50)
//   DEVSHARD_BENCH_REQS      total requests per scenario     (default 600)
//   DEVSHARD_BENCH_HOSTS     hosts per escrow group          (default 16)
//   DEVSHARD_BENCH_INPUT     prompt input length (tokens)    (default 2000)
//   DEVSHARD_BENCH_MAXTOK    max_tokens (long context)       (default 2000)
//   DEVSHARD_BENCH_CONC      comma list of concurrency levels(default 50,100,200,400)
//   DEVSHARD_BENCH_ESCROWS   comma list of escrow counts     (default 1,2)
//
// To approximate the customer's real numbers, set e.g.
//   DEVSHARD_BENCH_DELAY_MS=63000 DEVSHARD_BENCH_MAXTOK=4800
// (slow — prefer small delays and read the 1-vs-2-escrow *ratio*).

package user

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"devshard"
	"devshard/host"
	"devshard/internal/testutil"
	"devshard/logging"
	"devshard/signing"
	"devshard/state"
	"devshard/stub"
	"devshard/types"
)

// quietLogger drops everything except Error, so the per-host/per-escrow
// NewStateMachine INFO spam does not bury the result table.
type quietLogger struct{}

func (quietLogger) Info(string, ...any)  {}
func (quietLogger) Warn(string, ...any)  {}
func (quietLogger) Debug(string, ...any) {}
func (quietLogger) Error(msg string, kv ...any) {
	fmt.Fprintln(os.Stderr, append([]any{"ERROR", msg}, kv...)...)
}

// latencyEngine wraps a real stub engine and adds a fixed delay to every
// Execute call, simulating a slow (long-context) inference. The delay is
// applied where the real ML node call would block.
type latencyEngine struct {
	inner devshard.InferenceEngine
	delay time.Duration
}

func (e *latencyEngine) Execute(ctx context.Context, req devshard.ExecuteRequest) (*devshard.ExecuteResult, error) {
	if e.delay > 0 {
		t := time.NewTimer(e.delay)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return e.inner.Execute(ctx, req)
}

// buildEscrow constructs one fully independent devshard session (= one escrow):
// its own host group, state machines, and host.mu. Validation is disabled
// (ValidationRate=0) so the harness measures pure inference coordination.
func buildEscrow(t *testing.T, escrowID string, numHosts int, balance, grace uint64, delay time.Duration) *Session {
	t.Helper()

	hosts := make([]*signing.Secp256k1Signer, numHosts)
	for i := range hosts {
		hosts[i] = testutil.MustGenerateKey(t)
	}
	usr := testutil.MustGenerateKey(t)
	group := testutil.MakeGroup(hosts)

	config := types.SessionConfig{
		RefusalTimeout:   600,
		ExecutionTimeout: 3600,
		TokenPrice:       1,
		VoteThreshold:    uint32(numHosts) / 2,
		ValidationRate:   0, // isolate inference coordination from validation work
		CreateDevshardFee:  0,
		FeePerNonce:      0,
	}
	verifier := signing.NewSecp256k1Verifier()

	clients := make([]HostClient, numHosts)
	for i := range hosts {
		sm, err := state.NewStateMachine(escrowID, config, group, balance, usr.Address(), verifier)
		if err != nil {
			t.Fatalf("new host state machine: %v", err)
		}
		engine := &latencyEngine{inner: stub.NewInferenceEngine(), delay: delay}
		h, err := host.NewHost(sm, hosts[i], engine, escrowID, group, nil, host.WithGrace(grace))
		if err != nil {
			t.Fatalf("new host: %v", err)
		}
		clients[i] = &InProcessClient{Host: h}
	}

	userSM, err := state.NewStateMachine(escrowID, config, group, balance, usr.Address(), verifier)
	if err != nil {
		t.Fatalf("new user state machine: %v", err)
	}
	session, err := NewSession(userSM, usr, escrowID, group, clients, verifier)
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	return session
}

// loadResult holds the outcome of one (escrows, concurrency) scenario.
type loadResult struct {
	ok, failed int64
	lats       []time.Duration
	wall       time.Duration
}

// driveLoad fires totalReqs inferences using a pool of `concurrency` workers
// (the workers ARE the in-flight requests), round-robin across the given
// sessions. It records per-request latency and counts failures.
func driveLoad(sessions []*Session, params InferenceParams, totalReqs, concurrency int) loadResult {
	jobs := make(chan int, totalReqs)
	for i := 0; i < totalReqs; i++ {
		jobs <- i
	}
	close(jobs)

	latCh := make(chan time.Duration, totalReqs)
	var ok, failed int64
	var wg sync.WaitGroup
	ctx := context.Background()

	start := time.Now()
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				sess := sessions[idx%len(sessions)]
				t0 := time.Now()
				_, err := sess.SendInference(ctx, params)
				d := time.Since(t0)
				if err != nil {
					atomic.AddInt64(&failed, 1)
					continue
				}
				atomic.AddInt64(&ok, 1)
				latCh <- d
			}
		}()
	}
	wg.Wait()
	wall := time.Since(start)
	close(latCh)

	lats := make([]time.Duration, 0, totalReqs)
	for d := range latCh {
		lats = append(lats, d)
	}
	return loadResult{ok: ok, failed: failed, lats: lats, wall: wall}
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)) * p)
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

func TestEscrowConcurrency(t *testing.T) {
	logging.SetLogger(quietLogger{}) // silence per-host setup spam

	delay := time.Duration(envInt("DEVSHARD_BENCH_DELAY_MS", 50)) * time.Millisecond
	totalReqs := envInt("DEVSHARD_BENCH_REQS", 600)
	numHosts := envInt("DEVSHARD_BENCH_HOSTS", 16)
	inputLen := uint64(envInt("DEVSHARD_BENCH_INPUT", 2000))
	maxTokens := uint64(envInt("DEVSHARD_BENCH_MAXTOK", 2000))
	concLevels := envIntList("DEVSHARD_BENCH_CONC", []int{50, 100, 200, 400})
	escrowCounts := envIntList("DEVSHARD_BENCH_ESCROWS", []int{1, 2})

	prompt := makeLongPrompt(int(inputLen))
	params := InferenceParams{
		Model:       "bench-model",
		Prompt:      prompt,
		InputLength: inputLen,
		MaxTokens:   maxTokens,
		StartedAt:   1000,
	}

	t.Logf("")
	t.Logf("=== devshard escrow concurrency bench ===")
	t.Logf("simulated per-inference latency = %v", delay)
	t.Logf("total_reqs/scenario = %d   hosts/escrow = %d   input_len = %d   max_tokens = %d",
		totalReqs, numHosts, inputLen, maxTokens)
	t.Logf("NOTE: measures the devshard COORDINATION layer with a SIMULATED engine.")
	t.Logf("      It does NOT measure real GPU/vLLM capacity, host network, or the new-api gateway.")
	t.Logf("")
	t.Logf("%-8s %-6s %-10s %-7s %-7s %-12s %-12s %-12s %-12s",
		"escrows", "conc", "tput/s", "ok", "fail", "p50", "p95", "p99", "max")
	t.Logf("%s", strings.Repeat("-", 92))

	for _, ne := range escrowCounts {
		for _, conc := range concLevels {
			balance := uint64(1) << 62 // effectively unlimited: isolate concurrency, not depletion
			grace := uint64(totalReqs + 100)

			sessions := make([]*Session, ne)
			for e := 0; e < ne; e++ {
				sessions[e] = buildEscrow(t, fmt.Sprintf("escrow-%d", e), numHosts, balance, grace, delay)
			}

			res := driveLoad(sessions, params, totalReqs, conc)
			slices.Sort(res.lats)
			tput := float64(res.ok) / res.wall.Seconds()

			t.Logf("%-8d %-6d %-10.1f %-7d %-7d %-12v %-12v %-12v %-12v",
				ne, conc, tput, res.ok, res.failed,
				pct(res.lats, 0.50), pct(res.lats, 0.95), pct(res.lats, 0.99), pct(res.lats, 1.0))
		}
	}
	t.Logf("%s", strings.Repeat("-", 92))
	t.Logf("Read the tput/s column: if 2 escrows ~doubles tput at a given concurrency,")
	t.Logf("the coordination layer is the per-escrow bottleneck. If tput flattens with")
	t.Logf("rising concurrency on 1 escrow, that plateau is the single-escrow ceiling.")
}

// --- env helpers ---

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return def
}

func envIntList(key string, def []int) []int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var out []int
	for _, part := range strings.Split(v, ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(part)); err == nil {
			out = append(out, n)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

// makeLongPrompt builds a valid JSON chat body padded to roughly n bytes,
// standing in for a long-context request.
func makeLongPrompt(n int) []byte {
	if n < 1 {
		n = 1
	}
	const prefix = `{"messages":[{"role":"user","content":"`
	const suffix = `"}]}`
	return []byte(prefix + strings.Repeat("x", n) + suffix)
}

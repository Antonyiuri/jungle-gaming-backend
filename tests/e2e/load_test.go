//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

type loadOp struct {
	wallet   int
	provider string
	ext      string
	kind     string
	minor    int64
	ref      string
	viaSQS   bool
	// observed over HTTP
	status, failure, txID string
	latency               time.Duration
}

type reversibleOp struct {
	ext, kind string
	minor     int64
}

func money2(minor int64) string { return fmt.Sprintf("%d.%02d", minor/100, minor%100) }

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(float64(len(sorted)-1)*p)]
}

// The server's own answers are the facts; this test checks that they add up. After a burst of
// contended, duplicated and reversing operations arriving over HTTP and SQS through three processes,
// every wallet's balance is recomputed from the persisted transactions alone, independently of the
// ledger and of the code under test, and compared with what the system reports.
func TestSustainedMixedLoadKeepsEveryWalletConsistentAcrossInstances(t *testing.T) {
	runMixedLoad(t, false)
}

// The same load while instances are killed (SIGKILL) at random and restarted. Clients repeat the same
// identity after a failure, SQS redelivers what died in flight, and in the end every operation must have
// been applied exactly once: balances recomputed from the facts, ledger and events must all agree.
func TestChaosLoadKillingInstancesStillAppliesEveryOperationExactlyOnce(t *testing.T) {
	runMixedLoad(t, true)
}

func runMixedLoad(t *testing.T, chaos bool) {
	ensureInstancesReady(t)
	seconds := 20
	if chaos {
		seconds = 40
		setInputVisibility(t, 10) // killed consumers must give their messages back quickly
	}
	if v, err := strconv.Atoi(os.Getenv("E2E_LOAD_SECONDS")); err == nil && v > 0 {
		seconds = v
	}
	seed := uint64(time.Now().UnixNano())
	if v, err := strconv.ParseUint(os.Getenv("E2E_LOAD_SEED"), 10, 64); err == nil {
		seed = v
	}
	t.Logf("load: %d s, seed %d (set E2E_LOAD_SEED to replay the random choices)", seconds, seed)

	const nWallets = 10
	wallets := make([]wallet, nWallets)
	providers := make([]string, nWallets)
	ids := make([]string, nWallets)
	for i := range wallets {
		wallets[i] = newWallet(t, "120.00") // small balances keep wallets near zero, so overdraft protection is exercised constantly
		ids[i] = wallets[i].ID
		providers[i] = "provider-a"
		if i >= nWallets/2 {
			providers[i] = "provider-b"
		}
	}
	tokens := map[string]string{"provider-a": token(t, "provider-a"), "provider-b": token(t, "provider-b")}
	internal := token(t, "wallet-service")

	var failMu sync.Mutex
	var failures []string
	fail := func(format string, args ...any) {
		failMu.Lock()
		defer failMu.Unlock()
		if len(failures) < 40 {
			failures = append(failures, fmt.Sprintf(format, args...))
		}
	}

	var revMu sync.Mutex
	revs := make([][]reversibleOp, nWallets)
	addReversible := func(wi int, op loadOp) {
		revMu.Lock()
		revs[wi] = append(revs[wi], reversibleOp{ext: op.ext, kind: op.kind, minor: op.minor})
		revMu.Unlock()
	}
	pickReversible := func(wi int, rng *rand.Rand, betOnly bool) (reversibleOp, bool) {
		revMu.Lock()
		defer revMu.Unlock()
		var candidates []reversibleOp
		for _, r := range revs[wi] {
			if !betOnly || r.kind == "BET" {
				candidates = append(candidates, r)
			}
		}
		if len(candidates) == 0 {
			return reversibleOp{}, false
		}
		return candidates[rng.IntN(len(candidates))], true
	}

	var rr atomic.Uint64
	nextBase := func() string { return bases[int(rr.Add(1))%len(bases)] }
	// submit sends one command, retrying the same identity on transient failures like a real client.
	submit := func(op loadOp, base string) (response, error) {
		command := wallets[op.wallet].command(op.provider, op.kind, op.ext, money2(op.minor), op.ref)
		start := 0
		for i, b := range bases {
			if b == base {
				start = i
			}
		}
		// A real client keeps trying, in order, across the instances: with instances being killed two can be
		// down at once, and a few hundred milliseconds of attempts would not outlast a restart.
		attempts := 6
		if chaos {
			attempts = 60
		}
		var r response
		var err error
		for attempt := 0; attempt < attempts; attempt++ {
			r, err = wagerErr(bases[(start+attempt)%len(bases)], op.provider, tokens[op.provider], command, "k-"+op.ext)
			if err == nil && r.Status != 503 {
				return r, nil
			}
			time.Sleep(time.Duration(100+100*min(attempt, 5)) * time.Millisecond)
		}
		return r, err
	}

	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	const httpWorkers = 12
	opsByWorker := make([][]loadOp, httpWorkers)
	var wg sync.WaitGroup
	for id := 0; id < httpWorkers; id++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, uint64(id)+1))
			for time.Now().Before(deadline) {
				wi := rng.IntN(nWallets)
				op := loadOp{wallet: wi, provider: providers[wi], ext: uid("ld")}
				expectMismatch := false
				switch roll := rng.IntN(100); {
				case roll < 40:
					op.kind, op.minor = "BET", int64(50+rng.IntN(2450))
				case roll < 60:
					op.kind, op.minor = "WIN", int64(50+rng.IntN(3000))
				case roll < 68:
					op.kind = "LOSS"
				case roll < 80, roll >= 90 && roll < 95: // REFUND of a processed bet
					if ref, ok := pickReversible(wi, rng, true); ok {
						op.kind, op.ref, op.minor = "REFUND", ref.ext, ref.minor
						if roll >= 90 { // deliberately wrong amount: must be rejected deterministically
							op.minor, expectMismatch = ref.minor+1, true
						}
					} else {
						op.kind, op.minor = "BET", int64(50+rng.IntN(2450))
					}
				case roll < 90: // ROLLBACK of any processed bet, win or refund
					if ref, ok := pickReversible(wi, rng, false); ok {
						op.kind, op.ref, op.minor = "ROLLBACK", ref.ext, ref.minor
					} else {
						op.kind, op.minor = "WIN", int64(50+rng.IntN(3000))
					}
				default:
					op.kind, op.minor = "BET", int64(50+rng.IntN(2450))
				}

				duplicate := rng.IntN(100) < 12
				var dup response
				var dupErr error
				var dupWG sync.WaitGroup
				if duplicate { // the same identity arrives concurrently through another instance
					dupWG.Add(1)
					go func() { defer dupWG.Done(); dup, dupErr = submit(op, nextBase()) }()
				}
				started := time.Now()
				r, err := submit(op, nextBase())
				op.latency = time.Since(started)
				dupWG.Wait()
				if err != nil {
					fail("%s %s: transport error after retries: %v", op.kind, op.ext, err)
					continue
				}
				switch {
				case r.Status == 200 && r.str("status") == "PROCESSED":
					op.status = "PROCESSED"
				case r.Status == 422 && r.str("status") == "REJECTED":
					op.status, op.failure = "REJECTED", r.str("failureCode")
				default:
					fail("%s %s: unexpected response %d %s", op.kind, op.ext, r.Status, r.Body)
					continue
				}
				op.txID = r.str("transactionId")
				if expectMismatch && op.failure != "REFERENCE_AMOUNT_MISMATCH" {
					fail("REFUND %s with a wrong amount must be rejected with REFERENCE_AMOUNT_MISMATCH, got %s/%s", op.ext, op.status, op.failure)
				}
				if duplicate {
					switch {
					case dupErr != nil:
						fail("duplicate of %s: %v", op.ext, dupErr)
					case dup.Status != r.Status || dup.str("transactionId") != op.txID || dup.str("status") != r.str("status"):
						fail("duplicate of %s diverged: %d %s vs %d %s", op.ext, r.Status, r.Body, dup.Status, dup.Body)
					case !chaos && (r.field("idempotentReplay") == true) == (dup.field("idempotentReplay") == true):
						// (With instances being killed, the original answer may be lost and both copies become replays.)
						fail("exactly one of two concurrent copies of %s may execute (replay flags %v/%v)", op.ext, r.field("idempotentReplay"), dup.field("idempotentReplay"))
					}
				}
				if op.status == "PROCESSED" && op.kind != "LOSS" {
					addReversible(wi, op)
				}
				opsByWorker[id] = append(opsByWorker[id], op)
			}
		}()
	}

	// Two producers send bets and wins over SQS at the same time.
	var sqsMu sync.Mutex
	var sqsOps []loadOp
	for p := 0; p < 2; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, uint64(1000+p)))
			for time.Now().Before(deadline) {
				wi := rng.IntN(nWallets)
				op := loadOp{wallet: wi, provider: providers[wi], ext: uid("ldq"), viaSQS: true, kind: "BET", minor: int64(50 + rng.IntN(1500))}
				if rng.IntN(3) == 0 {
					op.kind, op.minor = "WIN", int64(50+rng.IntN(2000))
				}
				data := wallets[wi].command(op.provider, op.kind, op.ext, money2(op.minor), "")
				data["idempotencyKey"] = op.provider + ":" + op.ext
				if err := publishErr(op.provider, envelope(uuid.NewString(), data), wallets[wi].ID, publishOptions{Token: tokens[op.provider]}); err != nil {
					fail("sqs send %s: %v", op.ext, err)
					continue
				}
				sqsMu.Lock()
				sqsOps = append(sqsOps, op)
				sqsMu.Unlock()
				time.Sleep(time.Duration(20+rng.IntN(40)) * time.Millisecond)
			}
		}()
	}

	// The outbox backlog is sampled from the gauge operators would watch.
	var peakBacklog atomic.Int64
	stopSampler := make(chan struct{})
	var samplerWG sync.WaitGroup
	samplerWG.Add(1)
	go func() {
		defer samplerWG.Done()
		for {
			select {
			case <-stopSampler:
				return
			case <-time.After(2 * time.Second):
			}
			if v, err := metricErr(bases[0], "outbox_pending_events"); err == nil && int64(v) > peakBacklog.Load() {
				peakBacklog.Store(int64(v))
			}
		}
	}()

	// Reconciliation keeps running while writes are in flight: it must never observe a torn state.
	var reconciliations atomic.Int64
	stopReconcile := make(chan struct{})
	var reconcileWG sync.WaitGroup
	reconcileWG.Add(1)
	go func() {
		defer reconcileWG.Done()
		rng := rand.New(rand.NewPCG(seed, 9999))
		for {
			select {
			case <-stopReconcile:
				return
			case <-time.After(150 * time.Millisecond):
			}
			w := wallets[rng.IntN(nWallets)]
			r, err := callErr(nextBase(), "POST", "/wallets/"+w.ID+"/reconciliation", internal, nil, nil)
			if chaos && (err != nil || r.Status != 200) {
				continue // the instance asked may have just been killed
			}
			if err != nil || r.Status != 200 || r.field("consistent") != true {
				fail("reconciliation during load of %s: %v %d %s", w.ID, err, r.Status, r.Body)
			}
			reconciliations.Add(1)
		}
	}()

	// Chaos: every few seconds SIGKILL one instance, leave it dead for a moment, then start it again.
	// Only the Docker CLI is used, so the binary under test is the production one.
	var kills atomic.Int64
	if chaos {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, 31337))
			for time.Now().Before(deadline.Add(-5 * time.Second)) {
				time.Sleep(time.Duration(3000+rng.IntN(3000)) * time.Millisecond)
				victim := container(instances[rng.IntN(len(instances))])
				if err := exec.Command("docker", "stop", "-t", "0", victim).Run(); err != nil {
					fail("could not kill %s: %v", victim, err)
					return
				}
				kills.Add(1)
				time.Sleep(time.Duration(1000+rng.IntN(2000)) * time.Millisecond)
				if err := exec.Command("docker", "start", victim).Run(); err != nil {
					fail("could not restart %s: %v", victim, err)
					return
				}
			}
		}()
	}

	wg.Wait()
	loadEnded := time.Now()
	close(stopReconcile)
	reconcileWG.Wait()
	if chaos {
		t.Cleanup(func() { ensureInstancesReady(t) })
		ensureInstancesReady(t) // whatever was killed last must be back before the verification
		t.Logf("chaos: %d instance kills during the load", kills.Load())
		if kills.Load() == 0 {
			t.Fatal("the chaos run never killed an instance")
		}
	}
	if len(failures) > 0 {
		t.Fatalf("%d problems during the load (first %d shown):\n%v", len(failures), len(failures), failures)
	}

	var all []loadOp
	for _, ops := range opsByWorker {
		all = append(all, ops...)
	}
	httpOps := len(all)
	all = append(all, sqsOps...)

	// Every SQS message must reach a final state, whatever instance handled it.
	wait(t, 240*time.Second, "all SQS operations to reach a final state (killed consumers return their messages after the visibility timeout)", func() bool {
		return queryInt(t, "SELECT count(*) FROM wager_transactions WHERE wallet_id = ANY($1) AND status NOT IN ('PROCESSED','REJECTED')", ids) == 0 &&
			queryInt(t, "SELECT count(*) FROM wager_transactions WHERE wallet_id = ANY($1) AND origin='EXTERNAL'", ids) == len(all)
	})

	// 1. What clients were told matches what is stored.
	rejections := map[string]int{}
	var latencies []time.Duration
	for _, op := range all {
		var status, id string
		if err := pool.QueryRow(context.Background(), "SELECT status, id::text FROM wager_transactions WHERE provider_id=$1 AND external_transaction_id=$2", op.provider, op.ext).Scan(&status, &id); err != nil {
			t.Fatalf("operation %s %s is not stored: %v", op.kind, op.ext, err)
		}
		if !op.viaSQS {
			if status != op.status || id != op.txID {
				t.Fatalf("%s %s: the client was told %s/%s but %s/%s is stored", op.kind, op.ext, op.status, op.txID, status, id)
			}
			latencies = append(latencies, op.latency)
		}
		if status == "REJECTED" {
			var code string
			_ = pool.QueryRow(context.Background(), "SELECT COALESCE(failure_code,'') FROM wager_transactions WHERE id=$1", id).Scan(&code)
			rejections[code]++
		}
	}

	// 2. Each balance, recomputed from the persisted facts alone, equals the stored one.
	for i, w := range wallets {
		var expected, stored, version int64
		if err := pool.QueryRow(context.Background(), `SELECT COALESCE(SUM(CASE
				WHEN t.kind = 'OPENING' THEN t.amount_minor
				WHEN t.kind = 'BET' THEN -t.amount_minor
				WHEN t.kind IN ('WIN','REFUND') THEN t.amount_minor
				WHEN t.kind = 'ROLLBACK' AND r.kind = 'BET' THEN t.amount_minor
				WHEN t.kind = 'ROLLBACK' THEN -t.amount_minor
				ELSE 0 END), 0)::BIGINT
			FROM wager_transactions t LEFT JOIN wager_transactions r ON r.id = t.reference_transaction_id
			WHERE t.wallet_id = $1 AND t.status = 'PROCESSED'`, w.ID).Scan(&expected); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(context.Background(), "SELECT balance_minor, version FROM wallets WHERE id=$1", w.ID).Scan(&stored, &version); err != nil {
			t.Fatal(err)
		}
		if expected != stored || stored < 0 {
			t.Fatalf("wallet %d (%s): recomputed balance %d differs from the stored balance %d", i, w.ID, expected, stored)
		}
		if got := balanceMinor(t, w); got != money2(stored) {
			t.Fatalf("wallet %d: the API reports %s but %s is stored", i, got, money2(stored))
		}
		var entries, maxVersion, movements int64
		if err := pool.QueryRow(context.Background(), "SELECT count(*), COALESCE(max(wallet_version),0) FROM wallet_ledger_entries WHERE wallet_id=$1", w.ID).Scan(&entries, &maxVersion); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM wager_transactions WHERE wallet_id=$1 AND status='PROCESSED' AND kind <> 'LOSS'", w.ID).Scan(&movements); err != nil {
			t.Fatal(err)
		}
		if entries != movements || maxVersion != entries || version != entries {
			t.Fatalf("wallet %d: %d ledger entries, %d money movements, max version %d, wallet version %d must all agree", i, entries, movements, maxVersion, version)
		}
		reconcile(t, w)
	}
	if n := queryInt(t, "SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = ANY($1) AND (balance_after < 0 OR balance_before < 0)", ids); n != 0 {
		t.Fatalf("%d ledger entries show a negative balance", n)
	}

	// 3. Duplicates and redeliveries changed nothing: one stored transaction per submitted operation.
	if n := queryInt(t, "SELECT count(*) FROM wager_transactions WHERE wallet_id = ANY($1) AND origin='EXTERNAL'", ids); n != len(all) {
		t.Fatalf("%d operations were submitted but %d transactions are stored", len(all), n)
	}

	// 4. The outbox kept up: every event of every wallet reached the output queue, and the backlog gauge drained.
	for _, w := range wallets {
		assertEventsDelivered(t, w, 240*time.Second)
	}
	wait(t, 120*time.Second, "the outbox backlog gauge to drain", func() bool {
		v, err := metricErr(bases[0], "outbox_pending_events")
		return err == nil && v == 0
	})
	close(stopSampler)
	samplerWG.Wait()
	backlogDrained := time.Since(loadEnded)

	// 5. The lock-wait metric saw the contention (counters restart with a killed process, so only without chaos).
	var concurrencyConflicts float64
	if !chaos {
		var lockWaits float64
		for _, base := range bases {
			lockWaits += metric(t, base, "wallet_lock_wait_seconds_count")
			concurrencyConflicts += metric(t, base, "concurrency_conflicts_total")
		}
		if lockWaits < float64(len(all)) {
			t.Errorf("wallet_lock_wait_seconds counted %.0f lock acquisitions for %d operations", lockWaits, len(all))
		}
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	statuses := map[string]int{}
	for _, op := range all {
		if !op.viaSQS {
			statuses[op.status]++
		}
	}
	t.Logf("OBSERVATION (this machine, Docker VM, 3 instances; not a benchmark): %d operations in %d s (%d over HTTP, %d over SQS), %.0f HTTP ops/s", len(all), seconds, httpOps, len(sqsOps), float64(httpOps)/float64(seconds))
	t.Logf("HTTP latency: p50 %s, p95 %s, p99 %s, max %s", percentile(latencies, 0.50).Round(time.Millisecond), percentile(latencies, 0.95).Round(time.Millisecond), percentile(latencies, 0.99).Round(time.Millisecond), percentile(latencies, 1).Round(time.Millisecond))
	t.Logf("HTTP outcomes %v; stored rejections by code %v; %d reconciliations ran during the load", statuses, rejections, reconciliations.Load())
	if !chaos {
		t.Logf("concurrency conflicts (deadlock or serialization retries, summed over the instances): %.0f", concurrencyConflicts)
	}
	t.Logf("outbox backlog: sampled peak %d unpublished events during the load; everything published and the gauge back to 0 %s after the load ended", peakBacklog.Load(), backlogDrained.Round(time.Second))
}

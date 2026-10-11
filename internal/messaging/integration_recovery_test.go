//go:build integration

package messaging

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Antonyiuri/jungle-gaming-backend/internal/observability"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/store"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// A publisher that dies after the broker accepted a message but before the row was marked leaves
// the lease to expire. The next publisher must send the very same event ID again, which is what
// lets FIFO deduplication and consumers collapse the repeat.
func TestCrashBetweenSendAndMarkRepublishesTheSameEventIDs(t *testing.T) {
	pool := connect(t)
	s := store.New(pool)
	ctx := context.Background()
	w := newWallet(t, s, 10000)
	var mine []string
	rows, err := pool.Query(ctx, "SELECT id::text FROM outbox_events WHERE wallet_id=$1", w.ID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		mine = append(mine, id)
	}
	rows.Close()
	if len(mine) != 2 {
		t.Fatalf("expected the two opening events, got %d", len(mine))
	}
	isMine := map[string]bool{mine[0]: true, mine[1]: true}

	// First publisher: claims, sends, then "crashes" without calling Published.
	first := &fakeQueue{}
	sent := map[string]bool{}
	deadline := time.Now().Add(60 * time.Second)
	for len(sent) < len(mine) && time.Now().Before(deadline) {
		claimed, err := s.ClaimOutbox(ctx, 100, 300*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range claimed {
			if !isMine[r.ID] {
				continue
			}
			if _, err := first.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String("out"), MessageBody: aws.String(string(r.Payload)), MessageGroupId: aws.String(r.WalletID), MessageDeduplicationId: aws.String(r.ID)}); err != nil {
				t.Fatal(err)
			}
			sent[r.ID] = true
		}
	}
	if len(sent) != len(mine) {
		t.Fatalf("the first publisher never claimed its events: %v", sent)
	}
	var unmarked int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE wallet_id=$1 AND published_at IS NULL", w.ID).Scan(&unmarked); err != nil || unmarked != 2 {
		t.Fatalf("a crashed publisher leaves its events unmarked: %d (%v)", unmarked, err)
	}
	time.Sleep(500 * time.Millisecond) // the lease expires

	second := &fakeQueue{}
	p := &Publisher{Store: s, Queue: second, QueueURL: "output", Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: observability.New(), Interval: 10 * time.Millisecond}
	var pending int
	for deadline = time.Now().Add(60 * time.Second); time.Now().Before(deadline); {
		p.Once(ctx)
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE wallet_id=$1 AND published_at IS NULL", w.ID).Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			break
		}
	}
	if pending != 0 {
		t.Fatalf("the abandoned events were never taken over: %d pending", pending)
	}
	second.mu.Lock()
	defer second.mu.Unlock()
	resent := map[string]bool{}
	for _, in := range second.sent {
		if isMine[aws.ToString(in.MessageDeduplicationId)] {
			resent[aws.ToString(in.MessageDeduplicationId)] = true
		}
	}
	for id := range sent {
		if !resent[id] {
			t.Errorf("event %s was not republished with its original ID", id)
		}
	}
}

// Two publishers racing for the same backlog must never send an event twice in the same round.
func TestConcurrentPublishersDoNotDuplicateWork(t *testing.T) {
	pool := connect(t)
	s := store.New(pool)
	ctx := context.Background()
	for i := 0; i < 15; i++ {
		newWallet(t, s, 1000)
	}
	queues := []*fakeQueue{{}, {}, {}}
	var wg sync.WaitGroup
	for _, q := range queues {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := &Publisher{Store: s, Queue: q, QueueURL: "output", Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: observability.New(), Interval: 10 * time.Millisecond}
			for p.Once(ctx) > 0 {
			}
		}()
	}
	wg.Wait()
	seen := map[string]int{}
	for _, q := range queues {
		q.mu.Lock()
		for _, in := range q.sent {
			seen[aws.ToString(in.MessageDeduplicationId)]++
		}
		q.mu.Unlock()
	}
	if len(seen) < 30 {
		t.Fatalf("expected at least the 30 new events to be published, got %d", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("event %s was sent %d times by concurrent publishers", id, n)
		}
	}
}

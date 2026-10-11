package messaging

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Antonyiuri/jungle-gaming-backend/internal/auth"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/observability"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// ctxAwareQueue behaves like the real SDK: calls made with an expired context fail instead of succeeding.
type ctxAwareQueue struct {
	mu         sync.Mutex
	deleted    []string
	visibility map[string]int32
}

func (q *ctxAwareQueue) ReceiveMessage(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (q *ctxAwareQueue) DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.deleted = append(q.deleted, aws.ToString(in.ReceiptHandle))
	return &sqs.DeleteMessageOutput{}, nil
}

func (q *ctxAwareQueue) ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.visibility == nil {
		q.visibility = map[string]int32{}
	}
	q.visibility[aws.ToString(in.ReceiptHandle)] = in.VisibilityTimeout
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (q *ctxAwareQueue) SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	return &sqs.SendMessageOutput{}, nil
}

// stalledVerifier models an identity provider that hangs until the caller's deadline.
type stalledVerifier struct{}

func (stalledVerifier) Verify(ctx context.Context, _ string) (auth.Principal, error) {
	<-ctx.Done()
	return auth.Principal{}, ctx.Err()
}

// When the processing deadline is fully consumed, the retry delay must still be applied: it is
// recorded with its own context, otherwise the message would sit for the queue's whole visibility timeout.
func TestTimedOutProcessingStillDelaysTheRedelivery(t *testing.T) {
	q := &ctxAwareQueue{}
	c := &Consumer{Queue: q, QueueURL: "input", Verifier: stalledVerifier{}, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: observability.New(), Timeout: 100 * time.Millisecond}
	m := sqstypes.Message{
		MessageId: aws.String("m-1"), ReceiptHandle: aws.String("rh-1"), Body: aws.String("{}"),
		Attributes:        map[string]string{"MessageGroupId": "g", "ApproximateReceiveCount": "3"},
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{"Authorization": {DataType: aws.String("String"), StringValue: aws.String("Bearer token")}},
	}
	started := time.Now()
	c.handle(context.Background(), m)
	if time.Since(started) < 90*time.Millisecond {
		t.Fatal("the verifier should have consumed the processing deadline")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if got := q.visibility["rh-1"]; got != 20 {
		t.Fatalf("the third receive must be delayed by 20 seconds even after a timeout, got %d (visibility calls: %v)", got, q.visibility)
	}
	if len(q.deleted) != 0 {
		t.Fatalf("a timed-out message must not be acknowledged: %v", q.deleted)
	}
}

// pollRecorder is a queue whose ReceiveMessage waits to be released and records whether the caller
// cancelled the poll while it was in flight.
type pollRecorder struct {
	ctxAwareQueue
	started   chan struct{}
	release   chan struct{}
	cancelled chan bool
}

func (p *pollRecorder) ReceiveMessage(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	close(p.started)
	select {
	case <-p.release:
		p.cancelled <- ctx.Err() != nil
		return &sqs.ReceiveMessageOutput{}, nil
	case <-ctx.Done():
		p.cancelled <- true
		return nil, ctx.Err()
	}
}

// Shutdown must not abandon a poll in flight: the broker could hand the abandoned request a message
// that would then stay invisible for the whole visibility timeout.
func TestShutdownLetsTheInFlightPollFinishInsteadOfCancellingIt(t *testing.T) {
	q := &pollRecorder{started: make(chan struct{}), release: make(chan struct{}), cancelled: make(chan bool, 1)}
	c := &Consumer{Queue: q, QueueURL: "input", Verifier: stalledVerifier{}, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: observability.New(), Timeout: time.Second, WaitSecond: 1}
	ctx, stop := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() { defer close(finished); c.Run(ctx) }()
	<-q.started
	stop() // shutdown begins while the poll is waiting
	select {
	case <-finished:
		t.Fatal("Run must wait for the poll that is already in flight")
	case <-time.After(300 * time.Millisecond):
	}
	close(q.release)
	if <-q.cancelled {
		t.Fatal("the in-flight poll must not be cancelled by shutdown")
	}
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("Run must stop once the poll has finished and the context is done")
	}
}

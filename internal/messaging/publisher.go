package messaging

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Antonyiuri/jungle-gaming-backend/internal/observability"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/store"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

const (
	defaultSendTimeout = 10 * time.Second
	bookkeepingTimeout = 5 * time.Second
)

// Publisher relays committed outbox events to the output FIFO queue.
type Publisher struct {
	Store       *store.Store
	Queue       Queue
	QueueURL    string
	Log         *slog.Logger
	Metrics     *observability.Metrics
	Interval    time.Duration
	SendTimeout time.Duration // zero means 10 seconds
}

func (p *Publisher) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if p.Once(ctx) == 0 {
			sleep(ctx, p.Interval)
		}
	}
}

// Once claims and publishes one batch, returning how many events were claimed.
func (p *Publisher) Once(ctx context.Context) int {
	records, err := p.Store.ClaimOutbox(ctx, 10, 30*time.Second)
	if err != nil {
		if ctx.Err() == nil {
			p.Log.Error("outbox claim failed", "error", err)
			sleep(ctx, time.Second)
		}
		return 0
	}
	var wg sync.WaitGroup
	for _, r := range records {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.publish(ctx, r)
		}()
	}
	wg.Wait()
	return len(records)
}

// publish sends one event and records the outcome. Work already claimed is finished even while the
// process is shutting down, so the contexts are detached from ctx and bounded by their own timeouts.
func (p *Publisher) publish(ctx context.Context, r store.OutboxRecord) {
	timeout := p.SendTimeout
	if timeout <= 0 {
		timeout = defaultSendTimeout
	}
	sendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	_, err := p.Queue.SendMessage(sendCtx, &sqs.SendMessageInput{
		QueueUrl: aws.String(p.QueueURL), MessageBody: aws.String(string(r.Payload)),
		MessageGroupId: aws.String(r.WalletID), MessageDeduplicationId: aws.String(r.ID),
	})
	cancel()

	// A send that used its whole deadline leaves sendCtx expired, so recording the outcome needs a fresh budget.
	dbCtx, dbCancel := context.WithTimeout(context.WithoutCancel(ctx), bookkeepingTimeout)
	defer dbCancel()
	if err != nil {
		p.Metrics.Retries.WithLabelValues("outbox_publish").Inc()
		p.Log.Warn("outbox publish failed; scheduled retry", "eventId", r.ID, "error", err)
		if rerr := p.Store.RetryOutbox(dbCtx, r.ID, r.LeaseToken); rerr != nil {
			p.Log.Error("outbox retry scheduling failed; lease expiry will recover", "eventId", r.ID, "error", rerr)
		}
		return
	}
	if err := p.Store.Published(dbCtx, r.ID, r.LeaseToken); err != nil {
		p.Log.Error("outbox mark published failed; event may be republished with the same ID", "eventId", r.ID, "error", err)
	}
}

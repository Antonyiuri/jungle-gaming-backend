package messaging

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Antonyiuri/jungle-gaming-backend/internal/application"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/auth"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/domain"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/observability"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/store"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/trace"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// Queue is the SQS subset required by the consumer and publisher.
type Queue interface {
	ReceiveMessage(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

type Verifier interface {
	Verify(context.Context, string) (auth.Principal, error)
}

type Consumer struct {
	Queue      Queue
	QueueURL   string
	Verifier   Verifier
	Service    *application.Service
	Log        *slog.Logger
	Metrics    *observability.Metrics
	Timeout    time.Duration
	WaitSecond int32
}

// Run polls until ctx ends. Messages of one wallet group are handled sequentially; groups run concurrently.
//
// ctx stops the consumer from starting another poll, but a poll already in flight is allowed to finish
// (at most WaitSecond plus a margin) and its messages are handled. Cancelling a long poll is not safe: the
// broker can still hand a message to the abandoned request, and that message then stays invisible for the
// whole visibility timeout, delaying its wallet on every instance restart.
func (c *Consumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		pollCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Duration(c.WaitSecond)*time.Second+10*time.Second)
		out, err := c.Queue.ReceiveMessage(pollCtx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(c.QueueURL), MaxNumberOfMessages: 10, WaitTimeSeconds: c.WaitSecond,
			MessageAttributeNames:       []string{"All"},
			MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{"SenderId", "MessageGroupId", "ApproximateReceiveCount"},
		})
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				c.Log.Error("sqs receive failed", "error", err)
				c.Metrics.Retries.WithLabelValues("sqs_receive").Inc()
				sleep(ctx, 2*time.Second)
			}
			continue
		}
		groups := map[string][]sqstypes.Message{}
		for _, m := range out.Messages {
			g := m.Attributes["MessageGroupId"]
			groups[g] = append(groups[g], m)
		}
		var wg sync.WaitGroup
		for _, batch := range groups {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, m := range batch {
					c.handle(ctx, m)
				}
			}()
		}
		wg.Wait()
	}
}

type disposition int

const (
	ack disposition = iota
	retry
	dead
)

func (c *Consumer) handle(ctx context.Context, m sqstypes.Message) {
	log := c.Log.With("sqsMessageId", aws.ToString(m.MessageId))
	// A message already being handled finishes even during shutdown, bounded by the processing timeout.
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.Timeout)
	started := time.Now()
	d, err := c.process(pctx, m)
	cancel()
	c.Metrics.Duration.WithLabelValues("sqs").Observe(time.Since(started).Seconds())
	// The outcome is recorded with its own budget. A handler that used its whole deadline (a stalled
	// database, say) leaves pctx expired, and the delay for the retry could then never be applied.
	bctx, bcancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer bcancel()
	switch d {
	case ack:
		if _, err := c.Queue.DeleteMessage(bctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(c.QueueURL), ReceiptHandle: m.ReceiptHandle}); err != nil {
			log.Error("sqs delete failed; redelivery is safe", "error", err)
		}
	case retry:
		c.Metrics.Retries.WithLabelValues("consumer").Inc()
		log.Warn("transient failure; delaying redelivery", "error", err)
		count, _ := strconv.Atoi(m.Attributes["ApproximateReceiveCount"])
		if _, verr := c.Queue.ChangeMessageVisibility(bctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(c.QueueURL), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: backoff(count)}); verr != nil {
			log.Error("could not delay redelivery; the queue's visibility timeout applies", "error", verr)
		}
	default:
		// Never acknowledged: the broker's redrive policy moves it to the DLQ once it has been received
		// maxReceiveCount times. A permanent failure gains nothing from waiting out the queue's visibility
		// timeout (60 s), and FIFO keeps the rest of the wallet's messages behind it meanwhile, so the receives
		// are counted quickly instead.
		log.Warn("message rejected for dead-letter handling", "reason", err.Error())
		if _, verr := c.Queue.ChangeMessageVisibility(bctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(c.QueueURL), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: deadLetterDelay}); verr != nil {
			log.Error("could not shorten the delay of a dead-letter candidate; the queue's visibility timeout applies", "error", verr)
		}
	}
}

// deadLetterDelay is how long a permanently failed message stays invisible between receives, in seconds.
const deadLetterDelay int32 = 2

func (c *Consumer) process(ctx context.Context, m sqstypes.Message) (disposition, error) {
	attr, ok := m.MessageAttributes["Authorization"]
	if !ok || attr.StringValue == nil {
		return dead, errors.New("missing authorization attribute")
	}
	principal, err := c.Verifier.Verify(ctx, aws.ToString(attr.StringValue))
	if err != nil {
		if errors.Is(err, auth.ErrUnauthenticated) || errors.Is(err, auth.ErrForbidden) {
			return dead, err
		}
		return retry, err
	}
	envelope, err := ParseEnvelope([]byte(aws.ToString(m.Body)))
	if err != nil {
		return dead, err
	}
	if !strings.EqualFold(m.Attributes["MessageGroupId"], envelope.Data.WalletID) {
		return dead, errors.New("message group does not match wallet")
	}
	// SenderId is account-scoped on the local emulator, so the OIDC client is the stable sender identity.
	sender := principal.ClientID
	hash, err := envelope.Hash(sender)
	if err != nil {
		return dead, err
	}
	// The message ID is the correlation of everything this message causes, events included.
	ctx = trace.WithCorrelation(ctx, envelope.MessageID)
	result, err := c.Service.Process(ctx, principal, envelope.Data, &store.Delivery{MessageID: envelope.MessageID, ConsumerName: ConsumerName, Hash: hash, SenderID: sender})
	if err != nil {
		if code := domain.Code(err); code == "" || code == "TEMPORARILY_UNAVAILABLE" {
			return retry, err
		}
		return dead, err
	}
	c.Metrics.Results.WithLabelValues(result.Status, envelope.Data.Kind).Inc()
	if result.IdempotentReplay {
		c.Metrics.Replays.Inc()
	}
	c.Log.InfoContext(ctx, "wager message handled", "correlationId", envelope.MessageID, "messageId", envelope.MessageID, "walletId", envelope.Data.WalletID,
		"providerId", principal.ProviderID, "transactionId", result.TransactionID, "status", result.Status, "idempotentReplay", result.IdempotentReplay)
	if result.Status == domain.StatusFailed {
		return dead, errors.New("permanent failure persisted")
	}
	return ack, nil
}

// backoff maps the receive count to the 5, 10, 20, 40 second visibility schedule.
func backoff(receives int) int32 {
	if receives < 1 {
		receives = 1
	}
	if receives > 4 {
		receives = 4
	}
	return int32(5 << (receives - 1))
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

//go:build e2e

package e2e

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// The challenge asks for queue access to be controlled "by the broker's credentials and policies".
// Declaring policies proves nothing: this test checks, with the identities the provisioning created, that
// the broker refuses what each role must not do. LocalStack Community (compose.localstack.yaml) does not enforce IAM at all, so on
// that broker the test skips (or fails when E2E_REQUIRE_BROKER_IAM=1) instead of reporting a boundary that
// does not exist. The allowed side of the matrix is proven by the rest of this suite running on the same
// broker: the producers publish, the application consumes and publishes, the consumer and the operator read.

type queueCall struct {
	name string
	run  func(ctx context.Context, c *sqs.Client, queue string) error
}

var (
	sendCall = queueCall{"SendMessage", func(ctx context.Context, c *sqs.Client, queue string) error {
		_, err := c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(queue), MessageBody: aws.String(`{"policy":"matrix"}`),
			MessageGroupId: aws.String("policy-matrix"), MessageDeduplicationId: aws.String(uid("policy-matrix"))})
		return err
	}}
	receiveCall = queueCall{"ReceiveMessage", func(ctx context.Context, c *sqs.Client, queue string) error {
		// The SDK omits zero-valued integers, so 0 would fall back to the queue's 60 s visibility and 20 s long
		// poll. One second each keeps a probe from hiding a message or stalling the matrix.
		_, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(queue), MaxNumberOfMessages: 1, VisibilityTimeout: 1, WaitTimeSeconds: 1})
		return err
	}}
	deleteCall = queueCall{"DeleteMessage", func(ctx context.Context, c *sqs.Client, queue string) error {
		_, err := c.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(queue), ReceiptHandle: aws.String("policy-matrix-not-a-real-handle")})
		return err
	}}
	purgeCall = queueCall{"PurgeQueue", func(ctx context.Context, c *sqs.Client, queue string) error {
		_, err := c.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: aws.String(queue)})
		return err
	}}
	attributesCall = queueCall{"GetQueueAttributes", func(ctx context.Context, c *sqs.Client, queue string) error {
		_, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(queue), AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn}})
		return err
	}}
)

// denial classifies an SQS error: only an authorization refusal counts as "denied". Any other failure
// (missing queue, network) must not be mistaken for a working access control.
func denial(err error) (denied bool, code string) {
	if err == nil {
		return false, ""
	}
	var coded interface{ ErrorCode() string }
	if !errors.As(err, &coded) {
		return false, err.Error()
	}
	code = coded.ErrorCode()
	switch {
	case strings.Contains(code, "AccessDenied"), code == "UnrecognizedClientException", code == "InvalidClientTokenId", code == "InvalidAccessKeyId":
		return true, code
	}
	return false, code
}

func unknownKeyClient() *sqs.Client {
	return sqs.New(sqs.Options{Region: "us-east-1", BaseEndpoint: aws.String(sqsEndpoint),
		Credentials: credentials.NewStaticCredentialsProvider("AKIAUNKNOWNKEY0000X", "unknown-secret", "")})
}

func TestBrokerEnforcesQueuePermissionsPerIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	unknown := unknownKeyClient()

	// Gate: read-only calls that every enforcing broker must refuse. Nothing mutating runs unless all of
	// them were refused, so a non-enforcing broker is never asked to purge or fill a real queue.
	gate := []struct {
		who    string
		client *sqs.Client
		queue  string
	}{
		{"provider-a on the output queue", providerQ, outputQueue},
		{"events-consumer on the input queue", consumerQ, inputQueue},
		{"dlq-operator on the input queue", operatorQ, inputQueue},
		{"unknown access key on the input queue", unknown, inputQueue},
	}
	for _, g := range gate {
		err := attributesCall.run(ctx, g.client, g.queue)
		if ok, code := denial(err); !ok {
			message := "the broker did not refuse " + g.who + " (" + attributesCall.name + "; result: " + errText(err, code) + "): it does not enforce the declared policies"
			if os.Getenv("E2E_REQUIRE_BROKER_IAM") == "1" {
				t.Fatal(message)
			}
			t.Skip(message + " - run the stack with compose.ministack.yaml to prove the access control")
		}
	}

	cases := []struct {
		who    string
		client *sqs.Client
		call   queueCall
		queue  string
		qname  string
	}{
		// A provider may only publish requests.
		{"provider-a", providerQ, receiveCall, inputQueue, "input"},
		{"provider-a", providerQ, deleteCall, inputQueue, "input"},
		{"provider-a", providerQ, purgeCall, inputQueue, "input"},
		{"provider-a", providerQ, sendCall, outputQueue, "output"},
		{"provider-a", providerQ, receiveCall, outputQueue, "output"},
		{"provider-a", providerQ, receiveCall, inputDLQ, "input DLQ"},
		{"provider-a", providerQ, attributesCall, inputQueue, "input"},

		// The application worker consumes requests and publishes events; it must not forge requests,
		// read its own events, touch the dead-letter queues beyond metrics, or purge anything.
		{"jungle-worker", workerQ, sendCall, inputQueue, "input"},
		{"jungle-worker", workerQ, receiveCall, outputQueue, "output"},
		{"jungle-worker", workerQ, receiveCall, inputDLQ, "input DLQ"},
		{"jungle-worker", workerQ, sendCall, inputDLQ, "input DLQ"},
		{"jungle-worker", workerQ, purgeCall, inputQueue, "input"},
		{"jungle-worker", workerQ, purgeCall, outputQueue, "output"},

		// A downstream consumer reads events only: it cannot forge events nor reach the requests.
		{"events-consumer", consumerQ, sendCall, outputQueue, "output"},
		{"events-consumer", consumerQ, sendCall, inputQueue, "input"},
		{"events-consumer", consumerQ, receiveCall, inputQueue, "input"},
		{"events-consumer", consumerQ, purgeCall, outputQueue, "output"},

		// The DLQ operator drains dead letters and cannot inject traffic.
		{"dlq-operator", operatorQ, sendCall, inputQueue, "input"},
		{"dlq-operator", operatorQ, sendCall, outputQueue, "output"},
		{"dlq-operator", operatorQ, receiveCall, inputQueue, "input"},
		{"dlq-operator", operatorQ, receiveCall, outputQueue, "output"},
		{"dlq-operator", operatorQ, purgeCall, inputDLQ, "input DLQ"},

		// No credentials of this account, no access.
		{"unknown access key", unknown, sendCall, inputQueue, "input"},
		{"unknown access key", unknown, receiveCall, outputQueue, "output"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.who+" cannot "+c.call.name+" the "+c.qname+" queue", func(t *testing.T) {
			err := c.call.run(ctx, c.client, c.queue)
			if ok, code := denial(err); !ok {
				t.Fatalf("%s %s on the %s queue was not refused: %s", c.who, c.call.name, c.qname, errText(err, code))
			}
		})
	}

	// The permissions each role does need (read-only calls, so the matrix never disturbs live traffic).
	allowed := []struct {
		who    string
		client *sqs.Client
		queue  string
		qname  string
	}{
		{"jungle-worker", workerQ, inputQueue, "input"},
		{"jungle-worker", workerQ, outputQueue, "output"},
		{"jungle-worker", workerQ, inputDLQ, "input DLQ"},
		{"events-consumer", consumerQ, outputQueue, "output"},
		{"dlq-operator", operatorQ, inputDLQ, "input DLQ"},
		{"dlq-operator", operatorQ, outputDLQ, "output DLQ"},
	}
	for _, a := range allowed {
		a := a
		t.Run(a.who+" can read attributes of the "+a.qname+" queue", func(t *testing.T) {
			if err := attributesCall.run(ctx, a.client, a.queue); err != nil {
				t.Fatalf("%s was refused on the %s queue: %v", a.who, a.qname, err)
			}
		})
	}
	// Dead letters are read with the operator's own identity; the 1 s visibility leaves them in place.
	for _, dlq := range []string{inputDLQ, outputDLQ} {
		if err := receiveCall.run(ctx, operatorQ, dlq); err != nil {
			t.Errorf("dlq-operator could not read %s: %v", dlq, err)
		}
	}
}

func errText(err error, code string) string {
	if err == nil {
		return "success"
	}
	return code + ": " + err.Error()
}

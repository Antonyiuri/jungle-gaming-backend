package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/Antonyiuri/jungle-gaming-backend/internal/application"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/auth"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/config"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/httpapi"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/messaging"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/observability"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/store"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/workers"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

// Each module owns one concern and the resources it creates. Modules that hold a resource register their own
// lifecycle hooks, so a resource is released by the module that acquired it. Fx runs the stop hooks in reverse
// order of registration: the HTTP server stops first (draining, so readiness fails and no request starts),
// then every background worker, and the database pool last.
var (
	// Foundations other modules build on.
	observabilityModule = fx.Module("observability", fx.Provide(observability.New))
	databaseModule      = fx.Module("database", fx.Provide(newPool, newStore))
	applicationModule   = fx.Module("application", fx.Provide(application.New))

	// External systems. Integration tests replace these two with in-process substitutes.
	identityModule = fx.Module("identity", fx.Provide(newVerifier))
	brokerModule   = fx.Module("broker", fx.Provide(newBroker))

	// Background workers: each module provides its loop to the "workers" value group, and the workers module
	// supervises them all.
	consumerModule   = fx.Module("consumer", fx.Provide(newConsumer))
	publisherModule  = fx.Module("publisher", fx.Provide(newPublisher))
	referencesModule = fx.Module("references", fx.Provide(newReferences))
	observerModule   = fx.Module("observer", fx.Provide(newObserver))
	workersModule    = fx.Module("workers", fx.Invoke(runWorkers))

	// The HTTP server is registered last so that it is stopped first.
	httpModule = fx.Module("http", fx.Provide(newServer), fx.Invoke(serveHTTP))

	// serviceModules is everything that does not touch the network of another system by itself.
	serviceModules = fx.Options(observabilityModule, databaseModule, applicationModule,
		consumerModule, publisherModule, referencesModule, observerModule, workersModule, httpModule)
)

// Broker is the part of the SQS API the application uses; *sqs.Client implements it.
type Broker interface {
	messaging.Queue
	GetQueueAttributes(context.Context, *sqs.GetQueueAttributesInput, ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
}

// Worker is one supervised background loop.
type Worker struct {
	Name string
	Run  func(context.Context)
}

type workerResult struct {
	fx.Out
	Worker Worker `group:"workers"`
}

func newPool(lc fx.Lifecycle, c config.Config) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, c.DatabaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	lc.Append(fx.StopHook(func() { pool.Close() }))
	return pool, nil
}

func newStore(c config.Config, p *pgxpool.Pool, m *observability.Metrics) *store.Store {
	s := store.New(p)
	s.SetReferenceTTL(c.ReferenceTTL)
	s.OnLockWait(func(d time.Duration) { m.LockWait.Observe(d.Seconds()) })
	s.OnConflict(m.ConcurrencyConflicts.Inc)
	return s
}

// newVerifier retries OIDC discovery because the identity provider may still be starting.
func newVerifier(c config.Config) (httpapi.TokenVerifier, error) {
	var last error
	for i := 0; i < 20; i++ {
		v, err := auth.New(context.Background(), c)
		if err == nil {
			return v, nil
		}
		last = err
		time.Sleep(3 * time.Second)
	}
	return nil, last
}

func newBroker(c config.Config) (Broker, error) {
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion(c.Region))
	if err != nil {
		return nil, err
	}
	return sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		if c.SQSEndpoint != "" {
			o.BaseEndpoint = aws.String(c.SQSEndpoint)
		}
	}), nil
}

func newConsumer(c config.Config, q Broker, v httpapi.TokenVerifier, svc *application.Service, log *slog.Logger, m *observability.Metrics) workerResult {
	consumer := &messaging.Consumer{Queue: q, QueueURL: c.InputQueue, Verifier: v, Service: svc, Log: log, Metrics: m, Timeout: c.ProcessingTimeout, WaitSecond: 20}
	return workerResult{Worker: Worker{Name: "consumer", Run: consumer.Run}}
}

func newPublisher(c config.Config, st *store.Store, q Broker, log *slog.Logger, m *observability.Metrics) workerResult {
	publisher := &messaging.Publisher{Store: st, Queue: q, QueueURL: c.OutputQueue, Log: log, Metrics: m, Interval: time.Second}
	return workerResult{Worker: Worker{Name: "publisher", Run: publisher.Run}}
}

func newReferences(st *store.Store, log *slog.Logger) workerResult {
	refs := &workers.References{Store: st, Log: log, Interval: 2 * time.Second}
	return workerResult{Worker: Worker{Name: "references", Run: refs.Run}}
}

func newObserver(c config.Config, st *store.Store, q Broker, log *slog.Logger, m *observability.Metrics) workerResult {
	observer := &workers.Observer{Store: st, DLQDepth: dlqDepth(q, c.InputDLQ), Metrics: m, Log: log, Interval: 5 * time.Second}
	return workerResult{Worker: Worker{Name: "observer", Run: observer.Run}}
}

func newServer(c config.Config, svc *application.Service, v httpapi.TokenVerifier, pool *pgxpool.Pool, q Broker, log *slog.Logger, m *observability.Metrics) *httpapi.Server {
	ready := func(ctx context.Context) error {
		if err := pool.Ping(ctx); err != nil {
			return err
		}
		// Both directions matter: the consumer needs the input queue and the publisher the output queue.
		for _, queue := range []string{c.InputQueue, c.OutputQueue} {
			if _, err := q.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(queue)}); err != nil {
				return err
			}
		}
		return nil
	}
	return httpapi.New(c, svc, v, ready, log, m)
}

// dlqDepth reports the approximate number of messages waiting in the input dead-letter queue.
func dlqDepth(q Broker, url string) func(context.Context) (float64, error) {
	if url == "" {
		return nil
	}
	return func(ctx context.Context) (float64, error) {
		out, err := q.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(url), AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameApproximateNumberOfMessages},
		})
		if err != nil {
			return 0, err
		}
		n, err := strconv.ParseFloat(out.Attributes[string(sqstypes.QueueAttributeNameApproximateNumberOfMessages)], 64)
		return n, err
	}
}

// supervise runs a background loop until ctx ends. A panic is logged with its stack and the loop is
// restarted, so one bad message or row cannot take the HTTP API and the other workers down with it.
// Work interrupted by a panic is safe to repeat: leases expire and SQS redelivers.
func supervise(ctx context.Context, log *slog.Logger, name string, loop func(context.Context)) {
	for ctx.Err() == nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Error("worker panicked; restarting", "worker", name, "panic", r, "stack", string(debug.Stack()))
				}
			}()
			loop(ctx)
		}()
		if ctx.Err() == nil {
			select { // loops only return on cancellation; a restart delay avoids a hot panic loop
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
}

type workerSet struct {
	fx.In
	Lifecycle fx.Lifecycle
	Workers   []Worker `group:"workers"`
	Log       *slog.Logger
}

// runWorkers starts every worker of the group on application start and, on stop, cancels them together and
// waits for each to return, so a worker's database connections and in-flight broker calls are finished before
// the pool is closed. Each worker's exit is logged: a worker that does not stop in time is named.
func runWorkers(set workerSet) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var mu sync.Mutex
	running := map[string]bool{}
	set.Lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			for _, w := range set.Workers {
				mu.Lock()
				running[w.Name] = true
				mu.Unlock()
				wg.Add(1)
				go func() {
					defer wg.Done()
					supervise(ctx, set.Log, w.Name, w.Run)
					mu.Lock()
					delete(running, w.Name)
					mu.Unlock()
					set.Log.Info("worker stopped", "worker", w.Name)
				}()
				set.Log.Info("worker started", "worker", w.Name)
			}
			return nil
		},
		OnStop: func(stop context.Context) error {
			cancel()
			finished := make(chan struct{})
			go func() { wg.Wait(); close(finished) }()
			select {
			case <-finished:
				return nil
			case <-stop.Done():
				mu.Lock()
				stuck := make([]string, 0, len(running))
				for name := range running {
					stuck = append(stuck, name)
				}
				mu.Unlock()
				set.Log.Warn("workers did not stop before the shutdown deadline", "workers", stuck)
				return fmt.Errorf("workers still running at the shutdown deadline: %v", stuck)
			}
		},
	})
}

// serveHTTP binds the port during start, so a port that is already taken fails the start (and releases what
// the earlier modules acquired) instead of being discovered later by a goroutine.
func serveHTTP(lc fx.Lifecycle, sd fx.Shutdowner, c config.Config, srv *httpapi.Server, log *slog.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			listener, err := net.Listen("tcp", srv.HTTP.Addr)
			if err != nil {
				return fmt.Errorf("listen on %s: %w", srv.HTTP.Addr, err)
			}
			go func() {
				if err := srv.HTTP.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
					log.Error("http server failed", "error", err)
					_ = sd.Shutdown()
				}
			}()
			log.Info("server started", "addr", listener.Addr().String())
			return nil
		},
		OnStop: func(stop context.Context) error {
			srv.Drain()
			ctx, done := context.WithTimeout(stop, c.ShutdownTimeout)
			defer done()
			err := srv.HTTP.Shutdown(ctx)
			if err != nil {
				// Connections that did not finish in time must not keep the port or their goroutines.
				_ = srv.HTTP.Close()
			}
			return err
		},
	})
}

// Command server runs the HTTP API, the SQS consumer, the reference worker, the outbox publisher and the
// metrics observer as one process, composed with Uber Fx (see modules.go).
package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/Antonyiuri/jungle-gaming-backend/internal/config"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	fx.New(options(cfg, logger)).Run()
}

// options wires the whole application. It is separate from main so tests can validate the graph and start it
// with substitutes for the external systems (see settings and serviceModules: everything except those two).
func options(cfg config.Config, logger *slog.Logger) fx.Option {
	return fx.Options(
		settings(cfg, logger),
		identityModule, // the OIDC provider: needs network access to Keycloak
		brokerModule,   // the SQS broker: needs network access to the emulator
		serviceModules,
	)
}

// settings holds what every module shares: logging, the stop budget and the configuration.
func settings(cfg config.Config, logger *slog.Logger) fx.Option {
	return fx.Options(
		// Only Fx failures are logged; routine lifecycle events are suppressed.
		fx.WithLogger(func() fxevent.Logger {
			l := &fxevent.SlogLogger{Logger: logger}
			l.UseLogLevel(slog.LevelDebug)
			return l
		}),
		// The stop hooks must be allowed to use the whole configured shutdown budget.
		fx.StopTimeout(cfg.ShutdownTimeout+5*time.Second),
		fx.Supply(cfg, logger),
	)
}

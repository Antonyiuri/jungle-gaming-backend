package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr, DatabaseURL, Issuer, Audience, SQSEndpoint, InputQueue, OutputQueue, Region string
	// InputDLQ is optional: when set, the dead-letter depth is exported as a metric.
	InputDLQ                                         string
	ProviderClients                                  map[string]string
	InternalClient                                   string
	ProcessingTimeout, ShutdownTimeout, ReferenceTTL time.Duration
}

func Load() (Config, error) {
	c := Config{
		HTTPAddr: env("HTTP_ADDR", ":8080"), DatabaseURL: os.Getenv("DATABASE_URL"),
		Issuer: os.Getenv("OIDC_ISSUER_URL"), Audience: env("OIDC_AUDIENCE", "jungle-backend"),
		SQSEndpoint: os.Getenv("SQS_ENDPOINT_URL"), InputQueue: os.Getenv("SQS_INPUT_QUEUE_URL"), OutputQueue: os.Getenv("SQS_OUTPUT_QUEUE_URL"),
		InputDLQ: os.Getenv("SQS_INPUT_DLQ_URL"),
		Region:   env("AWS_REGION", "us-east-1"), InternalClient: env("INTERNAL_CLIENT_ID", "wallet-service"),
		ProviderClients: map[string]string{"provider-a": "provider-a", "provider-b": "provider-b"},
	}
	for name, value := range map[string]string{"DATABASE_URL": c.DatabaseURL, "OIDC_ISSUER_URL": c.Issuer, "SQS_INPUT_QUEUE_URL": c.InputQueue, "SQS_OUTPUT_QUEUE_URL": c.OutputQueue} {
		if value == "" {
			return c, fmt.Errorf("%s is required", name)
		}
	}
	if raw := os.Getenv("PROVIDER_CLIENT_MAP"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &c.ProviderClients); err != nil {
			return c, fmt.Errorf("invalid PROVIDER_CLIENT_MAP: %w", err)
		}
	}
	for client, provider := range c.ProviderClients {
		if client == "" || provider == "" || client == c.InternalClient {
			return c, fmt.Errorf("invalid provider client mapping")
		}
	}
	for name, target := range map[string]*time.Duration{"PROCESSING_TIMEOUT": &c.ProcessingTimeout, "SHUTDOWN_TIMEOUT": &c.ShutdownTimeout, "REFERENCE_TTL": &c.ReferenceTTL} {
		fallback := map[string]string{"PROCESSING_TIMEOUT": "10s", "SHUTDOWN_TIMEOUT": "30s", "REFERENCE_TTL": "24h"}[name]
		d, err := time.ParseDuration(env(name, fallback))
		if err != nil || d <= 0 {
			return c, fmt.Errorf("invalid %s", name)
		}
		*target = d
	}
	return c, nil
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

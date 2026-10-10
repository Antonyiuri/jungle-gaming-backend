package config

import (
	"testing"
	"time"
)

func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgres://app@localhost/db")
	t.Setenv("OIDC_ISSUER_URL", "http://idp/realms/x")
	t.Setenv("SQS_INPUT_QUEUE_URL", "http://q/in.fifo")
	t.Setenv("SQS_OUTPUT_QUEUE_URL", "http://q/out.fifo")
}

func TestLoadAppliesDefaults(t *testing.T) {
	setRequired(t)
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8080" || c.Audience != "jungle-backend" || c.Region != "us-east-1" || c.InternalClient != "wallet-service" {
		t.Errorf("unexpected defaults: %+v", c)
	}
	if c.ProcessingTimeout != 10*time.Second || c.ShutdownTimeout != 30*time.Second || c.ReferenceTTL != 24*time.Hour {
		t.Errorf("unexpected durations: %+v", c)
	}
	if c.ProviderClients["provider-a"] != "provider-a" || c.ProviderClients["provider-b"] != "provider-b" {
		t.Errorf("unexpected provider clients: %v", c.ProviderClients)
	}
}

// Each case runs as a subtest because t.Setenv lasts until the end of the (sub)test.
func TestLoadRequiresCriticalSettings(t *testing.T) {
	for _, name := range []string{"DATABASE_URL", "OIDC_ISSUER_URL", "SQS_INPUT_QUEUE_URL", "SQS_OUTPUT_QUEUE_URL"} {
		t.Run(name, func(t *testing.T) {
			setRequired(t)
			t.Setenv(name, "")
			if _, err := Load(); err == nil {
				t.Errorf("%s must be required", name)
			}
		})
	}
}

func TestLoadValidatesOverrides(t *testing.T) {
	invalid := map[string]map[string]string{
		"malformed provider map":        {"PROVIDER_CLIENT_MAP": "{"},
		"empty provider":                {"PROVIDER_CLIENT_MAP": `{"client":""}`},
		"internal client as a provider": {"PROVIDER_CLIENT_MAP": `{"wallet-service":"provider-x"}`},
		"bad duration":                  {"PROCESSING_TIMEOUT": "soon"},
		"zero duration":                 {"SHUTDOWN_TIMEOUT": "0s"},
		"negative duration":             {"REFERENCE_TTL": "-1h"},
	}
	for name, env := range invalid {
		t.Run(name, func(t *testing.T) {
			setRequired(t)
			for k, v := range env {
				t.Setenv(k, v)
			}
			if _, err := Load(); err == nil {
				t.Error("must be rejected")
			}
		})
	}
	t.Run("valid overrides", func(t *testing.T) {
		setRequired(t)
		t.Setenv("PROVIDER_CLIENT_MAP", `{"acme-client":"acme"}`)
		t.Setenv("PROCESSING_TIMEOUT", "3s")
		c, err := Load()
		if err != nil || c.ProviderClients["acme-client"] != "acme" || c.ProcessingTimeout != 3*time.Second {
			t.Errorf("valid overrides: %+v %v", c, err)
		}
	})
}

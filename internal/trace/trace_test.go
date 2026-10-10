package trace

import (
	"context"
	"strings"
	"testing"
)

func TestValidAcceptsOnlySafeIdentifiers(t *testing.T) {
	for _, id := range []string{"a", "request-001", "ce111fe2-27e5-4e0f-9a17-f6af0ae78f3d", "svc:op.1_x", strings.Repeat("a", 128)} {
		if !Valid(id) {
			t.Errorf("%q must be valid", id)
		}
	}
	for _, id := range []string{"", " ", "with space", "tab\there", "line\nbreak", "carriage\r", "nul\x00", "ünïcode", "semi;colon", "quote\"", strings.Repeat("a", 129)} {
		if Valid(id) {
			t.Errorf("%q must be refused", id)
		}
	}
}

func TestCorrelationRoundTripsAndIgnoresInvalidValues(t *testing.T) {
	ctx := WithCorrelation(context.Background(), "abc-1")
	if got := Correlation(ctx); got != "abc-1" {
		t.Fatalf("got %q", got)
	}
	if got := Correlation(WithCorrelation(ctx, "bad id")); got != "abc-1" {
		t.Fatalf("an invalid value must not replace the existing one, got %q", got)
	}
	if got := Correlation(context.Background()); got != "" {
		t.Fatalf("got %q", got)
	}
}

// Package trace carries request correlation identifiers across layers without coupling them.
package trace

import (
	"context"
	"regexp"
)

type key struct{}

// safe is deliberately narrow: identifiers end up in logs, response headers and event payloads,
// where control characters, spaces or unbounded length would be a liability.
var safe = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// Valid reports whether id may be propagated into logs, headers and events.
func Valid(id string) bool { return safe.MatchString(id) }

// WithCorrelation returns ctx carrying a correlation ID; invalid values are ignored.
func WithCorrelation(ctx context.Context, id string) context.Context {
	if !Valid(id) {
		return ctx
	}
	return context.WithValue(ctx, key{}, id)
}

// Correlation returns the correlation ID carried by ctx, or an empty string.
func Correlation(ctx context.Context) string {
	id, _ := ctx.Value(key{}).(string)
	return id
}

package analytics2ga4

import "context"

// Logger is the minimal logging interface required by the sender.
type Logger interface {
	Warningf(ctx context.Context, format string, args ...any)
	Errorf(ctx context.Context, format string, args ...any)
}

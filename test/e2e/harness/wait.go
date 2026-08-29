package harness

import (
	"context"
	"fmt"
	"time"
)

// WaitFor polls fn until it returns true or the timeout elapses.
func WaitFor(ctx context.Context, interval, timeout time.Duration, desc string, fn func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		ok, err := fn()
		if err != nil {
			lastErr = err
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for %s: %w", desc, ctx.Err())
		case <-time.After(interval):
		}
	}
	if lastErr != nil {
		return fmt.Errorf("wait for %s: %v", desc, lastErr)
	}
	return fmt.Errorf("wait for %s: timeout after %s", desc, timeout)
}

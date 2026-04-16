// pkg/client/publisher.go
package client

import (
	"context"
	"fmt"
	"time"
)

func (c *tachyonClient) Publish(ctx context.Context, topic string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("client is closed")
	}

	// The message envelope must match the ROUTER server's expectation:
	// Frame 1: Client Identity (added automatically by DEALER)
	// Frame 2: Empty delimiter (for ROUTER compatibility)
	// Frame 3: Topic
	// Frame 4: Payload
	if err := c.applyPublishTimeoutLocked(ctx); err != nil {
		return err
	}

	_, err := c.dealer.SendMessage("", topic, payload)
	if err != nil {
		return fmt.Errorf("failed to publish message to topic '%s': %w", topic, err)
	}

	return nil
}

func (c *tachyonClient) applyPublishTimeoutLocked(ctx context.Context) error {
	timeout := c.opts.Timeout
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < 0 {
			remaining = 0
		}
		if timeout <= 0 || remaining < timeout {
			timeout = remaining
		}
	}
	if timeout <= 0 {
		if err := c.dealer.SetSndtimeo(-1 * time.Millisecond); err != nil {
			return fmt.Errorf("failed to clear publish send timeout: %w", err)
		}
		return nil
	}
	if err := c.dealer.SetSndtimeo(timeout); err != nil {
		return fmt.Errorf("failed to apply publish send timeout: %w", err)
	}
	return nil
}

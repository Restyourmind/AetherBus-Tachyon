// pkg/client/publisher.go
package client

import (
	"context"
	"fmt"
)

func (c *tachyonClient) Publish(ctx context.Context, topic string, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("client is closed")
	}

	// The message envelope follows the AetherBus protocol:
	// Frame 1: Topic
	// Frame 2: Payload
	// The DEALER socket automatically adds its identity as the first frame.
	_, err := c.dealer.SendMessage(topic, payload)
	if err != nil {
		return fmt.Errorf("failed to publish message to topic '%s': %w", topic, err)
	}

	return nil
}

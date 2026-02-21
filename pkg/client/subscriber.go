// pkg/client/subscriber.go
package client

import (
	"context"
	"fmt"

	zmq "github.com/pebbe/zmq4"
)

func (c *tachyonClient) Subscribe(ctx context.Context, topic string, handler Handler) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("client is closed")
	}

	// Create a new SUB socket for this subscription.
	sub, err := c.ctx.NewSocket(zmq.SUB)
	if err != nil {
		return fmt.Errorf("failed to create subscriber socket: %w", err)
	}

	if err := sub.Connect(c.opts.SubAddr); err != nil {
		sub.Close()
		return fmt.Errorf("subscriber failed to connect to %s: %w", c.opts.SubAddr, err)
	}

	if err := sub.SetSubscribe(topic); err != nil {
		sub.Close()
		return fmt.Errorf("failed to subscribe to topic '%s': %w", topic, err)
	}

	// Run the message handler in a separate goroutine.
	go func() {
		defer sub.Close() // Ensure the socket is closed when the goroutine exits.
		for {
			select {
			case <-ctx.Done(): // Context cancellation stops the subscription.
				return
			default:
				// Poll for messages to avoid blocking indefinitely.
				msgs, err := sub.RecvMessageBytes(zmq.DONTWAIT)
				if err != nil {
					// ZMQ returns EAGAIN when no message is available.
					continue
				}

				// The AetherBus SUB protocol sends [Topic, Payload]
				if len(msgs) == 2 {
					// We ignore the topic from the message and use the one from the subscription.
					_ = handler(ctx, topic, msgs[1])
				}
			}
		}
	}()

	return nil
}

// Placeholder for Publish method to satisfy the Client interface
// The actual implementation is in publisher.go
func (c *tachyonClient) Publish(ctx context.Context, topic string, payload []byte) error {
	return nil
}

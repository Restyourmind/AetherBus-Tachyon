// pkg/client/subscriber.go
package client

import (
	"context"
	"fmt"
	"time"

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
		defer sub.Close()
		poller := zmq.NewPoller()
		poller.Add(sub, zmq.POLLIN)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				// Use Poller to wait for messages without consuming CPU
				sockets, err := poller.Poll(100 * time.Millisecond)
				if err != nil || len(sockets) == 0 {
					continue
				}

				msgs, err := sub.RecvMessageBytes(0) // No DONTWAIT needed with Poller
				if err == nil && len(msgs) == 2 { // Expect [Topic, Payload]
					_ = handler(ctx, string(msgs[0]), msgs[1])
				}
			}
		}
	}()

	return nil
}

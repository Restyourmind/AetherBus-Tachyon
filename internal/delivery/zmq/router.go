package zmq

import (
	"context"
	"fmt"

	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
	"github.com/pebbe/zmq4"
)

// Router is a ZeroMQ-based message router that listens for incoming events.
// It uses the ROUTER socket pattern to handle messages from multiple DEALER nodes.
type Router struct {
	bindAddress    string
	eventPublisher domain.EventPublisher
	socket         *zmq4.Socket
}

// NewRouter creates a new ZeroMQ Router.
func NewRouter(bindAddress string, publisher domain.EventPublisher) *Router {
	return &Router{
		bindAddress:    bindAddress,
		eventPublisher: publisher,
	}
}

// Start runs the main router loop in a dedicated goroutine.
// It listens for multi-part messages, reconstructs the event, and passes it
// to the application's usecase layer (EventPublisher).
func (r *Router) Start(ctx context.Context) error {
	// Create a new ROUTER socket
	sock, err := zmq4.NewSocket(zmq4.ROUTER)
	if err != nil {
		return fmt.Errorf("could not create zmq router socket: %w", err)
	}
	r.socket = sock

	// Bind to the specified address
	if err := r.socket.Bind(r.bindAddress); err != nil {
		return fmt.Errorf("could not bind zmq router to %s: %w", r.bindAddress, err)
	}

	fmt.Printf("ZMQ Router started and listening on %s\n", r.bindAddress)

	// Run the receiving loop in a goroutine
	go func() {
		for {
			// Check if the context has been cancelled
			select {
			case <-ctx.Done():
				fmt.Println("ZMQ Router stopping...")
				r.socket.Close()
				return
			default:
				// Non-blocking read of the message parts
				parts, err := r.socket.RecvMessageBytes(zmq4.DONTWAIT)
				if err != nil {
					// If there are no messages, EAGAIN is returned. We can ignore it.
					if zmq4.AsErrno(err) == zmq4.EAGAIN {
						continue // No message, just loop again
					}
					fmt.Printf("Error receiving ZMQ message: %v\n", err)
					continue
				}

				// Validate the multi-part message structure:
				// Frame 0: Identity, Frame 1: Empty Delimiter, Frame 2: Topic, Frame 3: Payload
				if len(parts) != 4 {
					fmt.Printf("Received malformed ZMQ message with %d parts\n", len(parts))
					continue
				}

				// identity := parts[0]
				topic := string(parts[2])
				payload := parts[3]

				// Create a domain event (in a real app, ID and Timestamp would be more robust)
				event := domain.Event{
					Topic:   topic,
					Payload: payload,
				}

				// Pass the event to the usecase layer for processing
				if err := r.eventPublisher.Publish(ctx, event); err != nil {
					fmt.Printf("Error publishing event from ZMQ router: %v\n", err)
				}
			}
		}
	}()

	return nil
}

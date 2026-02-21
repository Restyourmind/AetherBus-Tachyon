package zmq

import (
	"context"
	"fmt"
	"time"

	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
	"github.com/pebbe/zmq4"
)

// Router manages the ZMQ ROUTER socket for incoming events.
type Router struct {
	bindAddress string
	pubAddress  string
	publisher   domain.EventPublisher
	routerSocket *zmq4.Socket
	pubSocket    *zmq4.Socket
	codec       domain.Codec
	compressor  domain.Compressor
}

// NewRouter creates a new ZMQ Router.
func NewRouter(bindAddress, pubAddress string, publisher domain.EventPublisher, codec domain.Codec, compressor domain.Compressor) *Router {
	return &Router{
		bindAddress: bindAddress,
		pubAddress:  pubAddress,
		publisher:   publisher,
		codec:       codec,
		compressor:  compressor,
	}
}

// Start initializes and runs the ZMQ ROUTER socket loop.
func (r *Router) Start(ctx context.Context) error {
	// Create ROUTER socket
	routerSocket, err := zmq4.NewSocket(zmq4.ROUTER)
	if err != nil {
		return fmt.Errorf("failed to create router socket: %w", err)
	}
	r.routerSocket = routerSocket

	// Create PUB socket
	pubSocket, err := zmq4.NewSocket(zmq4.PUB)
	if err != nil {
		return fmt.Errorf("failed to create pub socket: %w", err)
	}
	r.pubSocket = pubSocket

	// Bind sockets
	if err := r.routerSocket.Bind(r.bindAddress); err != nil {
		return fmt.Errorf("failed to bind router socket: %w", err)
	}
	if err := r.pubSocket.Bind(r.pubAddress); err != nil {
		return fmt.Errorf("failed to bind pub socket: %w", err)
	}

	fmt.Println("ZMQ Router started")

	// Run the main loop in a goroutine
	go r.loop(ctx)

	return nil
}

// Stop gracefully closes the ZMQ socket.
func (r *Router) Stop() {
	if r.routerSocket != nil {
		r.routerSocket.Close()
	}
	if r.pubSocket != nil {
		r.pubSocket.Close()
	}
	fmt.Println("ZMQ Router stopped")
}

func (r *Router) loop(ctx context.Context) {
	defer r.Stop()

	poller := zmq4.NewPoller()
	poller.Add(r.routerSocket, zmq4.POLLIN)

	for {
		// Poll for events with a timeout
		sockets, err := poller.Poll(250 * time.Millisecond)
		if err != nil {
			// Break on context cancellation
			if ctx.Err() != nil {
				break
			}
			continue
		}

		if len(sockets) > 0 {
			msg, err := r.routerSocket.RecvMessageBytes(0)
			if err != nil {
				continue // Log error
			}

			if len(msg) < 2 {
				continue // Malformed
			}

			clientID := msg[0]
			rawEvent := msg[1]

			// 1. Decompress
			decompressedEvent, err := r.compressor.Decompress(rawEvent)
			if err != nil {
				continue // Log error
			}

			// 2. Decode
			var event domain.Event
			if err := r.codec.Decode(decompressedEvent, &event); err != nil {
				continue // Log error
			}

			envelope := domain.Envelope{
				ClientID: clientID,
				Event:    event,
			}

			// 3. Publish to the application logic
			if err := r.publisher.Publish(ctx, envelope); err == nil {
				// If successful, publish to the PUB socket (optional)
				// In a real scenario, you might want to re-encode and re-compress
				// before publishing.
				r.pubSocket.SendMessage(event.Topic, decompressedEvent)
			}
		}

		// Check for context cancellation
		if ctx.Err() != nil {
			break
		}
	}
}

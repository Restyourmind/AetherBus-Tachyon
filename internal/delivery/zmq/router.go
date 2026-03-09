package zmq

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode"

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

	codec      domain.Codec
	compressor domain.Compressor
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
	routerSocket, err := zmq4.NewSocket(zmq4.ROUTER)
	if err != nil {
		return fmt.Errorf("failed to create router socket: %w", err)
	}
	r.routerSocket = routerSocket

	pubSocket, err := zmq4.NewSocket(zmq4.PUB)
	if err != nil {
		return fmt.Errorf("failed to create pub socket: %w", err)
	}
	r.pubSocket = pubSocket

	if err := r.routerSocket.Bind(r.bindAddress); err != nil {
		return fmt.Errorf("failed to bind router socket: %w", err)
	}
	if err := r.pubSocket.Bind(r.pubAddress); err != nil {
		return fmt.Errorf("failed to bind pub socket: %w", err)
	}

	fmt.Println("ZMQ Router started")

	go r.loop(ctx)

	return nil
}

// Stop gracefully closes the ZMQ sockets.
func (r *Router) Stop() {
	if r.routerSocket != nil {
		_ = r.routerSocket.Close()
	}
	if r.pubSocket != nil {
		_ = r.pubSocket.Close()
	}
	fmt.Println("ZMQ Router stopped")
}

func (r *Router) loop(ctx context.Context) {
	defer r.Stop()

	poller := zmq4.NewPoller()
	poller.Add(r.routerSocket, zmq4.POLLIN)

	for {
		sockets, err := poller.Poll(250 * time.Millisecond)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			continue
		}

		if len(sockets) > 0 {
			msg, err := r.routerSocket.RecvMessageBytes(0)
			if err != nil {
				continue
			}

			clientID, topic, rawEvent, err := parseFrames(msg)
			if err != nil {
				continue
			}

			if err := validateTopic(topic); err != nil {
				fmt.Printf("invalid topic %q: %v\n", topic, err)
				continue
			}

			decompressedEvent, err := r.compressor.Decompress(rawEvent)
			if err != nil {
				fmt.Printf("failed to decompress event: %v\n", err)
				continue
			}

			var event domain.Event
			if err := r.codec.Decode(decompressedEvent, &event); err != nil {
				fmt.Printf("failed to decode event: %v\n", err)
				continue
			}

			event.Topic = topic

			envelope := domain.Envelope{
				ClientID: clientID,
				Event:    event,
			}

			if err := r.publisher.Publish(ctx, envelope); err != nil {
				fmt.Printf("failed to publish event: %v\n", err)
				continue
			}

			if _, err := r.pubSocket.SendMessage(event.Topic, decompressedEvent); err != nil {
				fmt.Printf("failed to fan out event on PUB socket: %v\n", err)
			}
		}

		if ctx.Err() != nil {
			break
		}
	}
}

func parseFrames(msg [][]byte) ([]byte, string, []byte, error) {
	switch {
	case len(msg) == 3:
		// [ClientID, Topic, Payload]
		return msg[0], string(msg[1]), msg[2], nil
	case len(msg) == 4 && len(msg[1]) == 0:
		// [ClientID, Delimiter, Topic, Payload]
		return msg[0], string(msg[2]), msg[3], nil
	default:
		return nil, "", nil, fmt.Errorf("malformed message: expected 3 frames or 4 frames with delimiter, got %d", len(msg))
	}
}

func validateTopic(topic string) error {
	if topic == "" {
		return fmt.Errorf("topic must not be empty")
	}

	if strings.HasPrefix(topic, ".") || strings.HasSuffix(topic, ".") || strings.Contains(topic, "..") {
		return fmt.Errorf("topic segments must be non-empty")
	}

	for _, r := range topic {
		if unicode.IsSpace(r) {
			return fmt.Errorf("topic must not contain whitespace")
		}
	}

	return nil
}

//go:build legacy
// +build legacy

package transport

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/aetherbus/aetherbus/core/errors"
	"github.com/aetherbus/aetherbus/core/message"
)

var (
	ErrTransportClosed = errors.ErrTransportClosed
)

// The Transport interface is the main interface for all transports.
// It is used by the client and the broker to send and receive messages.
// It is responsible for carrying the message from the sender to the receiver.
// It can be implemented using different protocols, like TCP, UDP, etc.
//
// The transport is a streaming, message-oriented, connection-based protocol.
// It means that it sends and receives messages in a stream, and that it
// maintains a connection between the sender and the receiver.
//
// The transport is also responsible for handling the connection status.
// It can be connected or disconnected.
// When the connection is lost, the transport will try to reconnect.
//
// The transport is also responsible for handling the message flow.
// It can be a client or a server.
// A client connects to a server.
// A server accepts connections from clients.
//
// The transport is also responsible for handling the message encoding.
// It uses a codec to encode and decode messages.
// The default codec is JSON.
//
// The transport is also responsible for handling the message compression.
// It uses a compressor to compress and decompress messages.
// The default compressor is LZ4.

// Transport is a generic interface for all transports.
type Transport interface {
	io.Closer
	// Connect connects to the remote endpoint.
	Connect(context.Context, string) (Channel, error)
}

// The Channel interface is the main interface for all channels.
// It is used by the client and the broker to send and receive messages.
// It is created by the transport.
//
// The channel is a full-duplex, message-oriented, connection-based protocol.
// It means that it can send and receive messages at the same time, and that it
// maintains a connection between the sender and the receiver.
//
// The channel is also responsible for handling the message flow.
// It can be a client or a server.
// A client connects to a server.
// A server accepts connections from clients.
//
// The channel is also responsible for handling the message encoding.
// It uses a codec to encode and decode messages.
// The default codec is JSON.
// The channel is also responsible for handling the message compression.
// It uses a compressor to compress and decompress messages.
// The default compressor is LZ4.

type Channel interface {
	io.Closer

	// Send sends a message.
	Send(context.Context, message.MessageRef) error
	// Receive receives a message.
	Receive(context.Context) (message.MessageRef, error)

	// ID returns the channel's unique identifier.
	ID() string
	// IsClosed returns true if the channel is closed.
	IsClosed() bool
}

// Listener is a generic interface for all listeners.
// It is used by the broker to accept connections from clients.
// It is created by the transport.
//
// The listener is a message-oriented, connection-based protocol.
// It means that it accepts connections from clients, and that it
// maintains a connection between the sender and the receiver.
//
// The listener is also responsible for handling the connection status.
// It can be connected or disconnected.
// When the connection is lost, the listener will try to reconnect.

type Listener interface {
	io.Closer
	// Accept accepts a new connection.
	Accept(context.Context) (Channel, error)
	// Addr returns the listener's network address.
	Addr() string
}

// NewDialer creates a new dialer.
// A dialer is a function that creates a new transport.
// It is used by the client to connect to the broker.
// It is also used by the broker to connect to other brokers.

type Dialer func(context.Context, string) (Channel, error)

// NewListener creates a new listener.
// A listener is a function that creates a new listener.
// It is used by the broker to accept connections from clients.
// It is also used by the broker to accept connections from other brokers.

type NewListenerFunc func(context.Context, string) (Listener, error)

var ( // TODO: use a proper registry
	mu       sync.Mutex
	dialers  = make(map[string]Dialer)
	liseners = make(map[string]NewListenerFunc)
)

func Register(scheme string, dialer Dialer, listener NewListenerFunc) {
	mu.Lock()
	defer mu.Unlock()

	if dialer == nil && listener == nil {
		panic("transport: Register dialer and listener are nil")
	}

	if _, dup := dialers[scheme]; dup {
		panic("transport: Register called twice for dialer " + scheme)
	}

	if _, dup := liseners[scheme]; dup {
		panic("transport: Register called twice for listener " + scheme)
	}

	dialers[scheme] = dialer
	liseners[scheme] = listener
}

func GetDialer(scheme string) (Dialer, error) {
	mu.Lock()
	defer mu.Unlock()

	dialer, ok := dialers[scheme]
	if !ok {
		return nil, fmt.Errorf("transport: unknown scheme %q", scheme)
	}

	return dialer, nil
}

func GetListener(scheme string) (NewListenerFunc, error) {
	mu.Lock()
	defer mu.Unlock()

	listener, ok := liseners[scheme]
	if !ok {
		return nil, fmt.Errorf("transport: unknown scheme %q", scheme)
	}

	return listener, nil
}

func Send(ctx context.Context, ch Channel, m message.MessageRef) error {
	deadline, ok := ctx.Deadline()
	if ok {
		m.ExpiresAt = deadline.UnixNano()
	} else {
		// TODO: use a default timeout?
		m.ExpiresAt = 0
	}
	return ch.Send(ctx, m)
}

// SendRecv sends a message and waits for a reply.
func SendRecv(ctx context.Context, ch Channel, m message.MessageRef) (message.MessageRef, error) {

	err := Send(ctx, ch, m)
	if err != nil {
		return nil, err
	}

	for {
		reply, err := ch.Receive(ctx)
		if err != nil {
			return nil, err
		}
		if reply.Id == m.Id {
			return reply, nil
		}
		// TODO: what to do with other messages?
	}
}

func SendWithTimeout(ch Channel, m message.MessageRef, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return Send(ctx, ch, m)
}

func SendRecvWithTimeout(ch Channel, m message.MessageRef, timeout time.Duration) (message.MessageRef, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return SendRecv(ctx, ch, m)
}

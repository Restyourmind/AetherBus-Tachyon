// pkg/client/client.go
package client

import (
	"context"
	"fmt"
	"sync"

	zmq "github.com/pebbe/zmq4"
)

// tachyonClient implements the Client interface.
type tachyonClient struct {
	opts   Options
	ctx    *zmq.Context
	dealer *zmq.Socket // For Publish and Request-Reply
	mu     sync.Mutex
	closed bool
}

// New creates a new instance of the Tachyon Client.
func New(opts ...Option) (Client, error) {
	options := defaultOptions()
	for _, opt := range opts {
		opt(&options)
	}

	zCtx, err := zmq.NewContext()
	if err != nil {
		return nil, fmt.Errorf("failed to create zmq context: %w", err)
	}

	dealer, err := zCtx.NewSocket(zmq.DEALER)
	if err != nil {
		zCtx.Term() // Clean up context on subsequent failure
		return nil, fmt.Errorf("failed to create dealer socket: %w", err)
	}

	// Set a client identity for the ROUTER to identify it.
	if options.NodeID != "" {
		dealer.SetIdentity(options.NodeID)
	}

	c := &tachyonClient{
		opts:   options,
		ctx:    zCtx,
		dealer: dealer,
	}

	if err := c.connect(); err != nil {
		c.Close() // Ensure resources are released on connection failure
		return nil, err
	}

	return c, nil
}

func (c *tachyonClient) connect() error {
	return c.dealer.Connect(c.opts.Addr)
}

func (c *tachyonClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	// Best-effort closing of resources
	if c.dealer != nil {
		c.dealer.Close()
	}
	if c.ctx != nil {
		c.ctx.Term()
	}

	c.closed = true
	return nil
}

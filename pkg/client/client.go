// pkg/client/client.go
package client

import (
	"context"
	"fmt"
    "time"
)

type tachyonClient struct {
	opts Options
	// zmqSocket จะถูกจัดการในนี้
}

// New สร้าง instance ใหม่ของ Tachyon Client
func New(opts ...Option) (Client, error) {
	options := Options{
		Addr:    "tcp://localhost:5555",
		Timeout: 5 * time.Second,
	}

	for _, opt := range opts {
		opt(&options)
	}

	c := &tachyonClient{
		opts: options,
	}

	if err := c.connect(); err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	return c, nil
}

func (c *tachyonClient) Publish(ctx context.Context, topic string, payload []byte) error {
	// Implementation logic สำหรับ ZMQ Dealer
	return nil
}

func (c *tachyonClient) Subscribe(ctx context.Context, topic string, handler Handler) error {
	// Implementation logic สำหรับ ZMQ Sub
	return nil
}

func (c *tachyonClient) Close() error {
	// Graceful shutdown
	return nil
}

func (c *tachyonClient) connect() error {
	// Logic การสร้าง ZMQ Connection
	return nil
}

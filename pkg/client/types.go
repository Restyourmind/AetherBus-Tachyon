// pkg/client/types.go
package client

import "context"

// Handler คือ function type สำหรับจัดการ message ที่รับเข้ามา
type Handler func(ctx context.Context, topic string, payload []byte) error

// Client 定義 interface สำหรับผู้ใช้งาน
type Client interface {
	Publish(ctx context.Context, topic string, payload []byte) error
	Subscribe(ctx context.Context, topic string, handler Handler) error
	Close() error
}

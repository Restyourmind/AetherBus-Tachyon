//go:build legacy
// +build legacy

package inproc

import (
	"context"
	"sync"

	"github.com/aetherbus/aetherbus/core/errors"
	"github.com/aetherbus/aetherbus/core/message"
	"github.com/aetherbus/aetherbus/core/transport"
)

func init() {
	transport.Register("inproc", Dial, Listen)
}

var ( // global listener registry so that inproc clients can find inproc listeners
	mu        sync.Mutex
	listeners = make(map[string]*listener)
)

// Dial connects to a listener.
func Dial(ctx context.Context, addr string) (transport.Channel, error) {
	mu.Lock()
	defer mu.Unlock()

	l, ok := listeners[addr]
	if !ok {
		return nil, errors.ErrNotConnected // no listener for this address
	}

	c1, c2 := transport.Pipe()
	l.accepted <- c1
	return c2, nil
}

// Listen creates a listener.
func Listen(ctx context.Context, addr string) (transport.Listener, error) {
	mu.Lock()
	defer mu.Unlock()

	if _, ok := listeners[addr]; ok {
		return nil, errors.ErrAddrInUse
	}

	l := &listener{
		addr:     addr,
		accepted: make(chan transport.Channel, 1),
		closed:   make(chan struct{}),
	}

	listeners[addr] = l

	go func() { // wait for the context to be cancelled and then close the listener
		<-ctx.Done()
		l.Close()
	}()

	return l, nil
}

type listener struct {
	addr     string
	accepted chan transport.Channel
	closed   chan struct{}
	once     sync.Once
}

func (l *listener) Accept(ctx context.Context) (transport.Channel, error) {
	select {
	case <-l.closed:
		return nil, transport.ErrListenerClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	case c := <-l.accepted:
		return c, nil
	}
}

func (l *listener) Addr() string {
	return l.addr
}

func (l *listener) Close() error {
	l.once.Do(func() {
		close(l.closed)
		mu.Lock()
		delete(listeners, l.addr)
		mu.Unlock()
	})
	return nil
}

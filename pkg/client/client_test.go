// pkg/client/client_test.go
package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	zmq "github.com/pebbe/zmq4"
)

// mockTachyonServer simulates the AetherBus ROUTER/PUB backend for testing.
type mockTachyonServer struct {
	ctx    *zmq.Context
	router *zmq.Socket // To receive from client's DEALER
	pub    *zmq.Socket // To publish to client's SUB
	stop   chan struct{}
	wg     sync.WaitGroup
}

// newMockServer creates and starts a new mock server.
func newMockServer(routerAddr, pubAddr string) (*mockTachyonServer, error) {
	ctx, err := zmq.NewContext()
	if err != nil {
		return nil, fmt.Errorf("mock server: failed to create context: %w", err)
	}

	router, err := ctx.NewSocket(zmq.ROUTER)
	if err != nil {
		ctx.Term()
		return nil, fmt.Errorf("mock server: failed to create router: %w", err)
	}

	if err := router.Bind(routerAddr); err != nil {
		router.Close()
		ctx.Term()
		return nil, fmt.Errorf("mock server: failed to bind router: %w", err)
	}

	pub, err := ctx.NewSocket(zmq.PUB)
	if err != nil {
		router.Close()
		ctx.Term()
		return nil, fmt.Errorf("mock server: failed to create pub socket: %w", err)
	}

	if err := pub.Bind(pubAddr); err != nil {
		pub.Close()
		router.Close()
		ctx.Term()
		return nil, fmt.Errorf("mock server: failed to bind pub socket: %w", err)
	}

	s := &mockTachyonServer{
		ctx:    ctx,
		router: router,
		pub:    pub,
		stop:   make(chan struct{}),
	}

	s.wg.Add(1)
	go s.run()
	return s, nil
}

// run is the main loop of the mock server.
// It receives a message from the client's DEALER (via ROUTER)
// and forwards it to the client's SUB (via PUB).
func (s *mockTachyonServer) run() {
	defer s.wg.Done()
	for {
		select {
		case <-s.stop:
			return
		default:
			// Non-blocking receive from the router
			msgs, err := s.router.RecvMessage(zmq.DONTWAIT)
			if err != nil { // EAGAIN means no message
				time.Sleep(10 * time.Millisecond)
				continue
			}

			// Expected format:
			// - [identity, topic, payload]
			// - [identity, empty-delimiter, topic, payload]
			if len(msgs) >= 3 {
				topicIndex := len(msgs) - 2
				payloadIndex := len(msgs) - 1
				topic := msgs[topicIndex]
				payload := msgs[payloadIndex]
				// Forward the message to subscribers via the PUB socket
				s.pub.SendMessage(topic, payload)
			}
		}
	}
}

// Close stops the server and cleans up resources.
func (s *mockTachyonServer) Close() {
	close(s.stop)
	s.wg.Wait()
	s.router.Close()
	s.pub.Close()
	s.ctx.Term()
}

func TestPublishAndSubscribe_HappyPath(t *testing.T) {
	dealerAddr := "inproc://test-dealer"
	subAddr := "inproc://test-sub"

	// 1. Start the Mock Server
	server, err := newMockServer(dealerAddr, subAddr)
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer server.Close()

	// Give the server a moment to bind the sockets
	time.Sleep(100 * time.Millisecond)

	// 2. Create the Client
	client, err := New(WithAddr(dealerAddr), WithSubAddr(subAddr))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// 3. Subscribe to a topic
	testTopic := "test.topic"
	testPayload := []byte("hello, world!")
	msgReceived := make(chan []byte, 1)

	handler := func(ctx context.Context, topic string, payload []byte) error {
		if topic != testTopic {
			t.Errorf("handler received unexpected topic: got %s, want %s", topic, testTopic)
		}
		msgReceived <- payload
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Subscribe(ctx, testTopic, handler); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Give the subscriber a moment to connect and subscribe
	time.Sleep(100 * time.Millisecond)

	// 4. Publish a message
	if err := client.Publish(ctx, testTopic, testPayload); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// 5. Assert the message was received
	select {
	case receivedPayload := <-msgReceived:
		if string(receivedPayload) != string(testPayload) {
			t.Errorf("handler received wrong payload: got %q, want %q", receivedPayload, testPayload)
		}
		// Test passed!
	case <-ctx.Done():
		t.Fatal("Test timed out waiting for message")
	}
}

func TestPublish_RespectsContextDeadline(t *testing.T) {
	dealerAddr := "inproc://test-dealer-context"
	subAddr := "inproc://test-sub-context"
	server, err := newMockServer(dealerAddr, subAddr)
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer server.Close()

	client, err := New(WithAddr(dealerAddr), WithSubAddr(subAddr), WithTimeout(time.Second))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = client.Publish(ctx, "orders.created", []byte(`{}`))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled error, got: %v", err)
	}
}

func TestPublish_AppliesSocketTimeoutFromOptions(t *testing.T) {
	dealerAddr := "inproc://test-dealer-timeout"
	subAddr := "inproc://test-sub-timeout"
	server, err := newMockServer(dealerAddr, subAddr)
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer server.Close()

	configuredTimeout := 250 * time.Millisecond
	client, err := New(WithAddr(dealerAddr), WithSubAddr(subAddr), WithTimeout(configuredTimeout))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	internalClient, ok := client.(*tachyonClient)
	if !ok {
		t.Fatalf("expected *tachyonClient, got %T", client)
	}

	sendTimeout, err := internalClient.dealer.GetSndtimeo()
	if err != nil {
		t.Fatalf("failed to get send timeout: %v", err)
	}
	if sendTimeout != configuredTimeout {
		t.Fatalf("expected send timeout %s, got %s", configuredTimeout, sendTimeout)
	}

	recvTimeout, err := internalClient.dealer.GetRcvtimeo()
	if err != nil {
		t.Fatalf("failed to get receive timeout: %v", err)
	}
	if recvTimeout != configuredTimeout {
		t.Fatalf("expected receive timeout %s, got %s", configuredTimeout, recvTimeout)
	}
}

func TestPublish_ClearsTemporaryDeadlineTimeoutWhenNoDefaultTimeout(t *testing.T) {
	dealerAddr := "inproc://test-dealer-timeout-clear"
	subAddr := "inproc://test-sub-timeout-clear"
	server, err := newMockServer(dealerAddr, subAddr)
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer server.Close()

	client, err := New(WithAddr(dealerAddr), WithSubAddr(subAddr))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctxWithDeadline, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if err := client.Publish(ctxWithDeadline, "orders.created", []byte("payload")); err != nil {
		t.Fatalf("Publish with deadline failed: %v", err)
	}

	internalClient, ok := client.(*tachyonClient)
	if !ok {
		t.Fatalf("expected *tachyonClient, got %T", client)
	}

	timeoutAfterDeadline, err := internalClient.dealer.GetSndtimeo()
	if err != nil {
		t.Fatalf("failed to get send timeout after deadline publish: %v", err)
	}
	if timeoutAfterDeadline <= 0 {
		t.Fatalf("expected temporary positive timeout after deadline publish, got %s", timeoutAfterDeadline)
	}

	if err := client.Publish(context.Background(), "orders.created", []byte("payload")); err != nil {
		t.Fatalf("Publish without deadline failed: %v", err)
	}

	timeoutAfterNoDeadline, err := internalClient.dealer.GetSndtimeo()
	if err != nil {
		t.Fatalf("failed to get send timeout after no-deadline publish: %v", err)
	}
	if timeoutAfterNoDeadline >= 0 {
		t.Fatalf("expected cleared send timeout (<0) after no-deadline publish, got %s", timeoutAfterNoDeadline)
	}
}

func TestSubscribe_HandlerReceivesActualPublishedTopic(t *testing.T) {
	dealerAddr := "inproc://test-dealer-topic"
	subAddr := "inproc://test-sub-topic"

	server, err := newMockServer(dealerAddr, subAddr)
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer server.Close()

	time.Sleep(100 * time.Millisecond)

	client, err := New(WithAddr(dealerAddr), WithSubAddr(subAddr))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	receivedTopic := make(chan string, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Subscribe(ctx, "orders.", func(ctx context.Context, topic string, payload []byte) error {
		receivedTopic <- topic
		return nil
	}); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	expectedTopic := "orders.created"
	if err := client.Publish(ctx, expectedTopic, []byte("payload")); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	select {
	case topic := <-receivedTopic:
		if topic != expectedTopic {
			t.Fatalf("handler received topic %q, expected %q", topic, expectedTopic)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for topic")
	}
}

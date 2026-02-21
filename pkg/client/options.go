// pkg/client/options.go
package client

import "time"

// Options holds configuration for the Tachyon client.
type Options struct {
	Addr    string        // Address for the DEALER socket
	SubAddr string        // Address for the SUB socket
	Timeout time.Duration // Connection/Request timeout
	NodeID  string        // Optional client identity for the DEALER socket
}

// defaultOptions returns a new Options struct with default values.
func defaultOptions() Options {
	return Options{
		Addr:    "tcp://localhost:5555", // Default DEALER address
		SubAddr: "tcp://localhost:5556", // Default SUB address
		Timeout: 5 * time.Second,
	}
}

// Option configures how we set up the client.
type Option func(*Options)

// WithAddr sets the DEALER socket address.
func WithAddr(addr string) Option {
	return func(o *Options) {
		o.Addr = addr
	}
}

// WithSubAddr sets the SUB socket address.
func WithSubAddr(addr string) Option {
	return func(o *Options) {
		o.SubAddr = addr
	}
}

// WithTimeout sets the client timeout.
func WithTimeout(t time.Duration) Option {
	return func(o *Options) {
		o.Timeout = t
	}
}

// WithNodeID sets the client's identity.
func WithNodeID(id string) Option {
	return func(o *Options) {
		o.NodeID = id
	}
}

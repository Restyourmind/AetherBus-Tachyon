// pkg/client/options.go
package client

import "time"

type Options struct {
	Addr         string
	Timeout      time.Duration
	Retries      int
	Compression  bool
}

type Option func(*Options)

func WithAddr(addr string) Option {
	return func(o *Options) {
		o.Addr = addr
	}
}

func WithTimeout(t time.Duration) Option {
	return func(o *Options) {
		o.Timeout = t
	}
}

func EnableCompression() Option {
	return func(o *Options) {
		o.Compression = true
	}
}

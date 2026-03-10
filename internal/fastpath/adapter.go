package fastpath

import "fmt"

// FrameAdapter defines a narrow adapter boundary for optional framing/compression offload.
//
// The adapter keeps Go as the default execution path and optionally delegates hot-path
// framing work to a sidecar process when explicitly enabled.
type FrameAdapter interface {
	EncodeFrame(flags byte, topic string, payload []byte) ([]byte, error)
	Mode() string
}

// GoOnlyAdapter keeps framing on the Go path.
type GoOnlyAdapter struct{}

// NewGoOnlyAdapter creates a Go-only framing adapter.
func NewGoOnlyAdapter() *GoOnlyAdapter {
	return &GoOnlyAdapter{}
}

// EncodeFrame encodes a frame using the in-process Go implementation.
func (a *GoOnlyAdapter) EncodeFrame(flags byte, topic string, payload []byte) ([]byte, error) {
	return encodeFrameLocal(flags, topic, payload), nil
}

// Mode returns the adapter mode identifier.
func (a *GoOnlyAdapter) Mode() string {
	return "go-only"
}

// SidecarAdapter offloads only selected operations to a Rust sidecar over unix sockets.
//
// Fallback-to-Go is explicit so default runtime reliability remains unchanged.
type SidecarAdapter struct {
	client         *Client
	cutoverBytes   int
	fallbackToGo   bool
	requireSidecar bool
}

// SidecarAdapterOptions controls sidecar offload behavior.
type SidecarAdapterOptions struct {
	// CutoverBytes defines payload size above which sidecar framing is preferred.
	// If unset/non-positive, SidecarLargePayloadCutover is used.
	CutoverBytes int
	// FallbackToGo allows immediate in-process fallback if sidecar calls fail.
	FallbackToGo bool
	// RequireSidecar enforces sidecar-only behavior above cutover (no fallback).
	RequireSidecar bool
}

// NewSidecarAdapter creates an adapter that can offload framing to sidecar.
func NewSidecarAdapter(client *Client, opts SidecarAdapterOptions) *SidecarAdapter {
	cutover := opts.CutoverBytes
	if cutover <= 0 {
		cutover = SidecarLargePayloadCutover
	}
	return &SidecarAdapter{
		client:         client,
		cutoverBytes:   cutover,
		fallbackToGo:   opts.FallbackToGo,
		requireSidecar: opts.RequireSidecar,
	}
}

// EncodeFrame offloads large payload framing to sidecar when configured, while preserving
// Go-only behavior by default.
func (a *SidecarAdapter) EncodeFrame(flags byte, topic string, payload []byte) ([]byte, error) {
	if len(payload) < a.cutoverBytes {
		return encodeFrameLocal(flags, topic, payload), nil
	}

	if a.client == nil {
		if a.requireSidecar {
			return nil, fmt.Errorf("sidecar required but not configured")
		}
		return encodeFrameLocal(flags, topic, payload), nil
	}

	out, err := a.client.encodeFrameSidecar(flags, topic, payload)
	if err == nil {
		return out, nil
	}

	if a.requireSidecar || !a.fallbackToGo {
		return nil, err
	}
	return encodeFrameLocal(flags, topic, payload), nil
}

// Mode returns the adapter mode identifier.
func (a *SidecarAdapter) Mode() string {
	return "rust-sidecar"
}

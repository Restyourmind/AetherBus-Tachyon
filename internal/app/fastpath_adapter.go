package app

import "github.com/aetherbus/aetherbus-tachyon/internal/fastpath"

// NewDefaultFrameAdapter returns the default Go-only framing adapter.
//
// This keeps broker orchestration/runtime behavior fully in Go unless an entrypoint
// explicitly opts into a sidecar adapter.
func NewDefaultFrameAdapter() fastpath.FrameAdapter {
	return fastpath.NewGoOnlyAdapter()
}

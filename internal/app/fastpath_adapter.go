package app

import (
	"fmt"

	"github.com/aetherbus/aetherbus-tachyon/config"
	"github.com/aetherbus/aetherbus-tachyon/internal/fastpath"
)

// NewDefaultFrameAdapter returns the default Go-only framing adapter.
//
// This keeps broker orchestration/runtime behavior fully in Go unless an entrypoint
// explicitly opts into a sidecar adapter.
func NewDefaultFrameAdapter() fastpath.FrameAdapter {
	return fastpath.NewGoOnlyAdapter()
}

// NewFrameAdapterFromConfig wires the fast-path adapter boundary from config.
//
// Default path remains Go-only. Sidecar mode activates only when explicitly enabled.
func NewFrameAdapterFromConfig(cfg *config.Config) (fastpath.FrameAdapter, func() error, error) {
	if cfg == nil || !cfg.FastpathSidecarEnabled {
		return NewDefaultFrameAdapter(), func() error { return nil }, nil
	}

	client, err := fastpath.Dial(cfg.FastpathSocketPath)
	if err != nil {
		if cfg.FastpathRequire {
			return nil, nil, fmt.Errorf("fastpath sidecar required but unreachable at %q: %w", cfg.FastpathSocketPath, err)
		}
		return NewDefaultFrameAdapter(), func() error { return nil }, nil
	}

	adapter := fastpath.NewSidecarAdapter(client, fastpath.SidecarAdapterOptions{
		CutoverBytes:   cfg.FastpathCutoverBytes,
		FallbackToGo:   cfg.FastpathFallbackToGo,
		RequireSidecar: cfg.FastpathRequire,
	})
	return adapter, client.Close, nil
}

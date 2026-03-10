package app

import (
	"testing"

	"github.com/aetherbus/aetherbus-tachyon/config"
)

func TestNewFrameAdapterFromConfig_DefaultGoOnly(t *testing.T) {
	adapter, closeFn, err := NewFrameAdapterFromConfig(&config.Config{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if adapter.Mode() != "go-only" {
		t.Fatalf("unexpected mode: %q", adapter.Mode())
	}
	if err := closeFn(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}

func TestNewFrameAdapterFromConfig_OptionalSidecarFallsBack(t *testing.T) {
	adapter, closeFn, err := NewFrameAdapterFromConfig(&config.Config{
		FastpathSidecarEnabled: true,
		FastpathSocketPath:     "/tmp/does-not-exist.sock",
		FastpathRequire:        false,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if adapter.Mode() != "go-only" {
		t.Fatalf("expected go-only fallback, got %q", adapter.Mode())
	}
	if err := closeFn(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}

func TestNewFrameAdapterFromConfig_RequiredSidecarErrors(t *testing.T) {
	_, _, err := NewFrameAdapterFromConfig(&config.Config{
		FastpathSidecarEnabled: true,
		FastpathSocketPath:     "/tmp/does-not-exist.sock",
		FastpathRequire:        true,
	})
	if err == nil {
		t.Fatalf("expected error when sidecar is required and unavailable")
	}
}

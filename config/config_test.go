package config

import "testing"

func TestLoadFastpathDefaults(t *testing.T) {
	t.Setenv("FASTPATH_SIDECAR_ENABLED", "")
	t.Setenv("FASTPATH_SOCKET_PATH", "")
	t.Setenv("FASTPATH_CUTOVER_BYTES", "")
	t.Setenv("FASTPATH_REQUIRE", "")
	t.Setenv("FASTPATH_FALLBACK_TO_GO", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	if cfg.FastpathSidecarEnabled {
		t.Fatalf("expected sidecar disabled by default")
	}
	if cfg.FastpathSocketPath != "/tmp/tachyon-fastpath.sock" {
		t.Fatalf("unexpected default socket path: %q", cfg.FastpathSocketPath)
	}
	if cfg.FastpathCutoverBytes != 256*1024 {
		t.Fatalf("unexpected default cutover: %d", cfg.FastpathCutoverBytes)
	}
	if cfg.FastpathRequire {
		t.Fatalf("expected fastpath require=false by default")
	}
	if !cfg.FastpathFallbackToGo {
		t.Fatalf("expected fastpath fallback=true by default")
	}
}

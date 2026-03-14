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

func TestLoadFastpathFromEnv(t *testing.T) {
	t.Setenv("FASTPATH_SIDECAR_ENABLED", "true")
	t.Setenv("FASTPATH_SOCKET_PATH", "/tmp/custom-fastpath.sock")
	t.Setenv("FASTPATH_CUTOVER_BYTES", "1048576")
	t.Setenv("FASTPATH_REQUIRE", "true")
	t.Setenv("FASTPATH_FALLBACK_TO_GO", "false")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	if !cfg.FastpathSidecarEnabled {
		t.Fatalf("expected sidecar enabled from env")
	}
	if cfg.FastpathSocketPath != "/tmp/custom-fastpath.sock" {
		t.Fatalf("unexpected socket path: %q", cfg.FastpathSocketPath)
	}
	if cfg.FastpathCutoverBytes != 1048576 {
		t.Fatalf("unexpected cutover: %d", cfg.FastpathCutoverBytes)
	}
	if !cfg.FastpathRequire {
		t.Fatalf("expected fastpath require=true from env")
	}
	if cfg.FastpathFallbackToGo {
		t.Fatalf("expected fastpath fallback=false from env")
	}
}

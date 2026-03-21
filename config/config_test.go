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

func TestLoadRouteCatalogDefaults(t *testing.T) {
	t.Setenv("ROUTE_CATALOG_PATH", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	if cfg.RouteCatalogPath != "./data/routes.catalog.json" {
		t.Fatalf("unexpected default route catalog path: %q", cfg.RouteCatalogPath)
	}
}

func TestLoadRouteCatalogFromEnv(t *testing.T) {
	t.Setenv("ROUTE_CATALOG_PATH", "/tmp/routes.snapshot.json")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	if cfg.RouteCatalogPath != "/tmp/routes.snapshot.json" {
		t.Fatalf("unexpected route catalog path: %q", cfg.RouteCatalogPath)
	}
}

func TestLoadDirectPriorityDefaults(t *testing.T) {
	t.Setenv("DIRECT_PRIORITY_CLASSES", "")
	t.Setenv("DIRECT_PRIORITY_WEIGHTS", "")
	t.Setenv("DIRECT_PRIORITY_PREEMPTION", "")
	t.Setenv("DIRECT_PRIORITY_BOOST_THRESHOLD", "")
	t.Setenv("DIRECT_PRIORITY_BOOST_OFFSET", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	if got := cfg.SupportedPriorityClasses; len(got) != 4 || got[0] != "urgent" || got[2] != "normal" {
		t.Fatalf("unexpected default priority classes: %#v", got)
	}
	if cfg.PriorityClassWeights["urgent"] <= cfg.PriorityClassWeights["normal"] {
		t.Fatalf("expected urgent to outrank normal: %#v", cfg.PriorityClassWeights)
	}
	if !cfg.PriorityPreemption {
		t.Fatalf("expected preemption enabled by default")
	}
	if cfg.PriorityBoostThreshold != 8 || cfg.PriorityBoostOffset != 1000 {
		t.Fatalf("unexpected boost defaults: threshold=%d offset=%d", cfg.PriorityBoostThreshold, cfg.PriorityBoostOffset)
	}
}

func TestLoadDirectPriorityFromEnv(t *testing.T) {
	t.Setenv("DIRECT_PRIORITY_CLASSES", "critical,bulk,background")
	t.Setenv("DIRECT_PRIORITY_WEIGHTS", "critical=9,bulk=4,background=1")
	t.Setenv("DIRECT_PRIORITY_PREEMPTION", "false")
	t.Setenv("DIRECT_PRIORITY_BOOST_THRESHOLD", "3")
	t.Setenv("DIRECT_PRIORITY_BOOST_OFFSET", "77")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	if got := cfg.SupportedPriorityClasses; len(got) != 3 || got[0] != "critical" || got[2] != "background" {
		t.Fatalf("unexpected configured priority classes: %#v", got)
	}
	if cfg.PriorityClassWeights["critical"] != 9 || cfg.PriorityClassWeights["background"] != 1 {
		t.Fatalf("unexpected configured weights: %#v", cfg.PriorityClassWeights)
	}
	if cfg.PriorityPreemption {
		t.Fatalf("expected preemption disabled from env")
	}
	if cfg.PriorityBoostThreshold != 3 || cfg.PriorityBoostOffset != 77 {
		t.Fatalf("unexpected configured boost policy: threshold=%d offset=%d", cfg.PriorityBoostThreshold, cfg.PriorityBoostOffset)
	}
}

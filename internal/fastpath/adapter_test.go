package fastpath

import "testing"

func TestGoOnlyAdapterEncodeFrame(t *testing.T) {
	adapter := NewGoOnlyAdapter()
	got, err := adapter.EncodeFrame(1, "orders.created", []byte("payload"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) == 0 {
		t.Fatalf("expected non-empty frame")
	}
	if adapter.Mode() != "go-only" {
		t.Fatalf("unexpected mode: %q", adapter.Mode())
	}
}

func TestSidecarAdapterFallsBackToGoWhenClientMissing(t *testing.T) {
	adapter := NewSidecarAdapter(nil, SidecarAdapterOptions{
		CutoverBytes: 1,
		FallbackToGo: true,
	})
	got, err := adapter.EncodeFrame(2, "orders.created", []byte("large-payload"))
	if err != nil {
		t.Fatalf("expected fallback success, got error: %v", err)
	}
	if len(got) == 0 {
		t.Fatalf("expected non-empty frame")
	}
}

func TestSidecarAdapterRequireSidecarErrorsWhenMissing(t *testing.T) {
	adapter := NewSidecarAdapter(nil, SidecarAdapterOptions{
		CutoverBytes:   1,
		RequireSidecar: true,
	})
	_, err := adapter.EncodeFrame(2, "orders.created", []byte("large-payload"))
	if err == nil {
		t.Fatalf("expected error when sidecar is required and missing")
	}
}

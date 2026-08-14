package server

import "testing"

func TestCertManagerEmbeddedFallback(t *testing.T) {
	m, err := NewCertManager("", "")
	if err != nil {
		t.Fatalf("embedded keypair must load: %v", err)
	}
	if m.pinned {
		t.Error("embedded cert must not be pinned (auto-refresh should apply)")
	}
	cert, err := m.GetCertificate(nil)
	if err != nil || cert == nil {
		t.Fatalf("GetCertificate: %v, %v", cert, err)
	}
	if cert.Leaf == nil {
		t.Error("leaf must be parsed for expiry logging")
	}
}

func TestCertManagerPinned(t *testing.T) {
	if _, err := NewCertManager("/does/not/exist.pem", "/does/not/exist.key"); err == nil {
		t.Error("missing pinned cert paths must error, not silently fall back")
	}
}

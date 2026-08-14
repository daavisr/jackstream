package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

// The bundled *.local-ip.medicmobile.org cert is a Let's Encrypt cert that
// rotates every ~60 days. Instead of requiring an image rebuild on every
// rotation, CertManager re-fetches it from the issuing service at runtime and
// hot-swaps it via tls.Config.GetCertificate. The embedded copy only bridges
// startup when the service is unreachable.
const (
	certRefreshBase     = "https://local-ip.medicmobile.org"
	certRefreshInterval = 12 * time.Hour
	certFetchTimeout    = 15 * time.Second
	certFetchMaxSize    = 1 << 20
	certExpiryWarning   = 14 * 24 * time.Hour
)

type CertManager struct {
	cur    atomic.Pointer[tls.Certificate]
	pinned bool // cert supplied via HTTPS_CERT_PATH/KEY_PATH; never auto-refresh
}

// NewCertManager loads the user-provided keypair when both paths are set
// (pinned: the user manages renewal), otherwise the embedded local-ip cert.
func NewCertManager(certPath, keyPath string) (*CertManager, error) {
	m := &CertManager{}
	if certPath != "" && keyPath != "" {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, err
		}
		m.pinned = true
		m.set(&cert)
		return m, nil
	}
	cert, err := tls.X509KeyPair(CertPEM, KeyPEM)
	if err != nil {
		return nil, err
	}
	m.set(&cert)
	return m, nil
}

func (m *CertManager) set(cert *tls.Certificate) {
	if leaf, err := x509.ParseCertificate(cert.Certificate[0]); err == nil {
		cert.Leaf = leaf
	}
	m.cur.Store(cert)
}

func (m *CertManager) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return m.cur.Load(), nil
}

// StartAutoRefresh refreshes the local-ip cert immediately and then every
// certRefreshInterval until ctx is cancelled. No-op for pinned certs.
func (m *CertManager) StartAutoRefresh(ctx context.Context) {
	if m.pinned {
		return
	}
	go func() {
		m.refresh(ctx)
		t := time.NewTicker(certRefreshInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.refresh(ctx)
			}
		}
	}()
}

func (m *CertManager) refresh(ctx context.Context) {
	cert, err := fetchKeypair(ctx)
	if err != nil {
		log.Printf("[https] cert refresh failed: %v", err)
		m.warnIfExpiring()
		return
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		log.Printf("[https] refreshed cert unparsable: %v", err)
		return
	}
	if cur := m.cur.Load(); cur != nil && cur.Leaf != nil && !leaf.NotAfter.After(cur.Leaf.NotAfter) {
		return // current cert is at least as fresh
	}
	m.set(&cert)
	log.Printf("[https] cert refreshed, valid until %s", leaf.NotAfter.Format(time.RFC3339))
}

func (m *CertManager) warnIfExpiring() {
	cur := m.cur.Load()
	if cur == nil || cur.Leaf == nil {
		return
	}
	left := time.Until(cur.Leaf.NotAfter)
	if left < certExpiryWarning {
		log.Printf("[https] WARNING: serving cert expires in %s (%s) and refresh is failing",
			left.Round(time.Hour), cur.Leaf.NotAfter.Format(time.RFC3339))
	}
}

func fetchKeypair(ctx context.Context) (tls.Certificate, error) {
	certPEM, err := fetchPEM(ctx, certRefreshBase+"/fullchain")
	if err != nil {
		return tls.Certificate{}, err
	}
	keyPEM, err := fetchPEM(ctx, certRefreshBase+"/key")
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair(certPEM, keyPEM)
}

func fetchPEM(ctx context.Context, url string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, certFetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("%s -> HTTP %d", url, res.StatusCode)
	}
	return io.ReadAll(io.LimitReader(res.Body, certFetchMaxSize))
}

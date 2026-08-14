package main

import (
	"context"
	"crypto/tls"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/lborruto/jackstream/internal/bt"
	"github.com/lborruto/jackstream/internal/server"
)

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

func main() {
	port := envInt("PORT", 7000)
	httpsPort := envInt("HTTPS_PORT", 7001)

	btClient, err := bt.NewClient()
	if err != nil {
		log.Fatalf("bt.NewClient: %v", err)
	}

	srv := server.New(btClient)
	handler := srv.BuildHandler()

	// Root context cancels the background goroutines (torrent cleanup, cert
	// refresh) on shutdown.
	rootCtx, stop := context.WithCancel(context.Background())
	defer stop()

	go func() {
		t := time.NewTicker(60 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-rootCtx.Done():
				return
			case <-t.C:
				btClient.Cleanup(srv.MaxConcurrent())
			}
		}
	}()

	httpSrv := &http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: handler,
	}
	go func() {
		log.Printf("jackstream listening on :%d", port)
		if err := httpSrv.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("[http] serve: %v", err)
		}
	}()

	var httpsSrv *http.Server
	if os.Getenv("HTTPS_DISABLED") != "1" && os.Getenv("HTTPS_DISABLED") != "true" {
		cm, cmErr := server.NewCertManager(os.Getenv("HTTPS_CERT_PATH"), os.Getenv("HTTPS_KEY_PATH"))
		if cmErr != nil {
			log.Printf("[https] disabled (%v)", cmErr)
		} else {
			cm.StartAutoRefresh(rootCtx)
			httpsSrv = &http.Server{
				Addr:      ":" + strconv.Itoa(httpsPort),
				Handler:   handler,
				TLSConfig: &tls.Config{GetCertificate: cm.GetCertificate},
			}
			go func() {
				log.Printf("jackstream https listening on :%d", httpsPort)
				if err := httpsSrv.ListenAndServeTLS("", ""); err != http.ErrServerClosed {
					log.Printf("[https] serve: %v", err)
				}
			}()
		}
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("received signal, shutting down")

	stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
	if httpsSrv != nil {
		_ = httpsSrv.Shutdown(ctx)
	}
	btClient.Shutdown()
}

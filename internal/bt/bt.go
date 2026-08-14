package bt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/lborruto/jackstream/internal/store"
)

var videoExt = map[string]string{
	".mkv":  "video/x-matroska",
	".mp4":  "video/mp4",
	".m4v":  "video/mp4",
	".mov":  "video/quicktime",
	".avi":  "video/x-msvideo",
	".webm": "video/webm",
	".ts":   "video/mp2t",
}

type activeEntry struct {
	t          *torrent.Torrent
	lastAccess time.Time
	readers    int
}

type Client struct {
	mu      sync.Mutex
	client  *torrent.Client
	active  map[string]*activeEntry
	dataDir string
}

var (
	ErrNotFound  = errors.New("torrent_not_found")
	ErrFetchFail = errors.New("torrent_fetch_failed")
)

// Sanity caps for the Jackett-proxied .torrent download.
const (
	torrentFetchTimeout = 30 * time.Second
	torrentFetchMaxSize = 32 << 20
)

func streamReadyTimeout() time.Duration {
	s, _ := strconv.Atoi(os.Getenv("STREAM_READY_TIMEOUT_S"))
	if s <= 0 {
		s = 60
	}
	return time.Duration(s) * time.Second
}

func idleTimeout() time.Duration {
	m, _ := strconv.Atoi(os.Getenv("TORRENT_IDLE_TIMEOUT_MIN"))
	if m <= 0 {
		m = 30
	}
	return time.Duration(m) * time.Minute
}

func NewClient() (*Client, error) {
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = os.Getenv("DATA_DIR")
	if cfg.DataDir == "" {
		cfg.DataDir = "/tmp/jackstream"
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir data dir: %w", err)
	}
	cfg.NoDHT = true
	cfg.Seed = false
	cfg.DisableUTP = true
	c, err := torrent.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{
		client:  c,
		active:  make(map[string]*activeEntry),
		dataDir: cfg.DataDir,
	}, nil
}

// dropLocked drops the torrent from the client and returns the on-disk root
// path of its data ("" if unknown). Must be called with c.mu held; the caller
// removes the entry from c.active and deletes the returned path AFTER
// releasing the lock, so slow disk I/O never blocks stream requests.
func (c *Client) dropLocked(e *activeEntry) string {
	// Capture the info name BEFORE Drop — after Drop, Info() may become nil.
	var rootPath string
	if info := e.t.Info(); info != nil && info.Name != "" && c.dataDir != "" {
		if p := filepath.Join(c.dataDir, info.Name); p != c.dataDir && p != "/" {
			rootPath = p
		}
	}
	e.t.Drop()
	return rootPath
}

func removePaths(paths []string) {
	for _, p := range paths {
		if p == "" {
			continue
		}
		if err := os.RemoveAll(p); err != nil {
			log.Printf("[bt] removeAll %q: %v", p, err)
		}
	}
}

func fetchTorrentBuffer(ctx context.Context, url string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, torrentFetchTimeout)
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
		return nil, fmt.Errorf(".torrent fetch %d", res.StatusCode)
	}
	return io.ReadAll(io.LimitReader(res.Body, torrentFetchMaxSize))
}

func (c *Client) addNewTorrent(ctx context.Context, meta store.TorrentMeta) (*torrent.Torrent, error) {
	if meta.TorrentURL != "" {
		buf, err := fetchTorrentBuffer(ctx, meta.TorrentURL)
		if err == nil {
			mi, err := metainfo.Load(bytes.NewReader(buf))
			if err == nil {
				t, err := c.client.AddTorrent(mi)
				if err == nil {
					return t, nil
				}
				log.Printf("[bt] AddTorrent from .torrent failed: %v, trying magnet", err)
			} else {
				log.Printf("[bt] metainfo.Load failed: %v, trying magnet", err)
			}
		} else {
			log.Printf("[bt] .torrent fetch failed: %v, trying magnet", err)
		}
	}
	if meta.MagnetURI != "" {
		return c.client.AddMagnet(meta.MagnetURI)
	}
	return nil, ErrFetchFail
}

func (c *Client) AddTorrent(ctx context.Context, torrentID string) (*torrent.Torrent, error) {
	c.mu.Lock()
	if e, ok := c.active[torrentID]; ok {
		e.lastAccess = time.Now()
		t := e.t
		c.mu.Unlock()
		return t, nil
	}
	c.mu.Unlock()

	meta, ok := store.Get(torrentID)
	if !ok {
		return nil, ErrNotFound
	}

	if meta.InfoHash != "" {
		ih := metainfo.NewHashFromHex(meta.InfoHash)
		for _, t := range c.client.Torrents() {
			if t.InfoHash() == ih {
				c.mu.Lock()
				c.active[torrentID] = &activeEntry{t: t, lastAccess: time.Now()}
				c.mu.Unlock()
				return t, nil
			}
		}
	}

	t, err := c.addNewTorrent(ctx, meta)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.active[torrentID] = &activeEntry{t: t, lastAccess: time.Now()}
	c.mu.Unlock()
	return t, nil
}

func pickVideoFile(t *torrent.Torrent) *torrent.File {
	var videos []*torrent.File
	for _, f := range t.Files() {
		ext := strings.ToLower(path.Ext(f.DisplayPath()))
		if _, ok := videoExt[ext]; ok {
			videos = append(videos, f)
		}
	}
	pool := videos
	if len(pool) == 0 {
		pool = t.Files()
	}
	if len(pool) == 0 {
		return nil
	}
	best := pool[0]
	for _, f := range pool[1:] {
		if f.Length() > best.Length() {
			best = f
		}
	}
	return best
}

func mimeFor(name string) string {
	if ct, ok := videoExt[strings.ToLower(path.Ext(name))]; ok {
		return ct
	}
	return "video/x-matroska"
}

func (c *Client) StreamFile(w http.ResponseWriter, r *http.Request, torrentID string) error {
	t, err := c.AddTorrent(r.Context(), torrentID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error":"torrent_not_found","message":"Stream session expired. Please go back and select the stream again."}`))
			return nil
		}
		if errors.Is(err, ErrFetchFail) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			w.Write([]byte(`{"error":"torrent_fetch_failed"}`))
			return nil
		}
		return err
	}

	select {
	case <-t.GotInfo():
	case <-time.After(streamReadyTimeout()):
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"error":"stream_timeout"}`))
		return nil
	case <-r.Context().Done():
		return nil
	}

	file := pickVideoFile(t)
	if file == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"no_video_file"}`))
		return nil
	}
	// Deliberately no file.Download() here: the reader below drives piece
	// prioritization, so we only fetch what playback (plus readahead) needs
	// instead of downloading the whole file in the background.
	reader := file.NewReader()
	defer reader.Close()
	reader.SetReadahead(16 << 20)
	reader.SetResponsive()

	c.readerInc(torrentID)
	defer c.readerDec(torrentID)

	w.Header().Set("Content-Type", mimeFor(file.DisplayPath()))
	http.ServeContent(w, r, filepath.Base(file.DisplayPath()), time.Time{}, reader)
	return nil
}

func (c *Client) readerInc(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.active[id]; ok {
		e.readers++
		e.lastAccess = time.Now()
	}
}

func (c *Client) readerDec(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.active[id]; ok {
		if e.readers > 0 {
			e.readers--
		}
		e.lastAccess = time.Now()
	}
}

func (c *Client) Cleanup(maxConcurrent int) {
	idle := idleTimeout()
	var doomed []string

	c.mu.Lock()
	now := time.Now()
	for id, e := range c.active {
		if e.readers == 0 && now.Sub(e.lastAccess) > idle {
			doomed = append(doomed, c.dropLocked(e))
			delete(c.active, id)
		}
	}
	if len(c.active) > maxConcurrent {
		type cand struct {
			id string
			ts time.Time
		}
		var cands []cand
		for id, e := range c.active {
			if e.readers == 0 {
				cands = append(cands, cand{id, e.lastAccess})
			}
		}
		sort.Slice(cands, func(i, j int) bool { return cands[i].ts.Before(cands[j].ts) })
		for _, x := range cands {
			if len(c.active) <= maxConcurrent {
				break
			}
			doomed = append(doomed, c.dropLocked(c.active[x.id]))
			delete(c.active, x.id)
		}
	}
	c.mu.Unlock()

	removePaths(doomed)
}

func (c *Client) Shutdown() {
	c.mu.Lock()
	for id, e := range c.active {
		c.dropLocked(e)
		delete(c.active, id)
	}
	if c.client != nil {
		_ = c.client.Close()
		c.client = nil
	}
	c.mu.Unlock()

	// Wiping the whole data dir covers every torrent's files, including
	// orphans from fetch-fail paths.
	if c.dataDir != "" && c.dataDir != "/" {
		if err := os.RemoveAll(c.dataDir); err != nil {
			log.Printf("[bt] shutdown removeAll %q: %v", c.dataDir, err)
		}
	}
}

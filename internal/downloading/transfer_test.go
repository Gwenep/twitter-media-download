package downloading

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
)

func TestTransferStallRemovesPartialFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("partial"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	dir := t.TempDir()
	path := filepath.Join(dir, "video.mp4")
	err := transferMediaOnce(context.Background(), resty.New(), srv.URL, path, 100*time.Millisecond)
	if !errors.Is(err, errMediaIdle) {
		t.Fatalf("expected idle error, got %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("partial file left behind: %v %v", entries, err)
	}
}

func TestTransferContinuousSlowDataAndAtomicPublish(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "video.mp4")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < 8; i++ {
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Error("final filename visible before completion")
			}
			w.Write([]byte("chunk"))
			w.(http.Flusher).Flush()
			time.Sleep(30 * time.Millisecond)
		}
	}))
	defer srv.Close()
	if err := transferMediaOnce(context.Background(), resty.New(), srv.URL, path, 150*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || len(data) != 40 {
		t.Fatalf("incomplete final file: bytes=%d err=%v", len(data), err)
	}
}

func TestTransferHTTPErrorDoesNotPublish(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("not a video"))
	}))
	defer srv.Close()
	path := filepath.Join(t.TempDir(), "video.mp4")
	if err := transferMediaOnce(context.Background(), resty.New(), srv.URL, path, time.Second); err == nil {
		t.Fatal("expected HTTP error")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("HTTP error published as media")
	}
}

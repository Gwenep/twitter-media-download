package downloading

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Gwenep/twitter-media-download/internal/utils"
	"github.com/go-resty/resty/v2"
	log "github.com/sirupsen/logrus"
)

var errMediaIdle = errors.New("媒体下载连续无数据")

// 进度日志只保留简短文件名，完整名称仍用于实际保存。
func transferDisplayName(path string) string {
	name := []rune(filepath.Base(path))
	if len(name) > 24 {
		return string(name[:24]) + "……"
	}
	return string(name)
}

func transferProgress(received, total int64, speed float64) string {
	rate := fmt.Sprintf("%.0f KB/s", speed/1024)
	if speed >= 1048576 {
		rate = fmt.Sprintf("%.1f MB/s", speed/1048576)
	}
	if total <= 0 {
		return fmt.Sprintf("已下载 %.1f MB · %s", float64(received)/1048576, rate)
	}
	percent := float64(received) / float64(total) * 100
	if percent > 100 {
		percent = 100
	}
	filled := int(percent / 5)
	return fmt.Sprintf("%3.0f%% [%s%s] %.1f/%.1f MB · %s", percent,
		strings.Repeat("=", filled), strings.Repeat("-", 20-filled),
		float64(received)/1048576, float64(total)/1048576, rate)
}

// 超时按连续无数据计算，慢速但持续传输的大文件不会因总用时过长被取消。
func transferMedia(ctx context.Context, client *resty.Client, url, path string) error {
	return transferMediaWithIdle(ctx, client, url, path, 90*time.Second)
}

func transferMediaWithIdle(ctx context.Context, client *resty.Client, url, path string, idle time.Duration) error {
	for attempt := 1; ; attempt++ {
		err := transferMediaOnce(ctx, client, url, path, idle)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !errors.Is(err, errMediaIdle) || attempt >= 3 {
			return err
		}
		log.Warnf("下载停滞，重新连接重试（%d/3） %s", attempt+1, transferDisplayName(path))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

type transferReader struct {
	io.Reader
	last  *atomic.Int64
	bytes *atomic.Int64
}

func (r transferReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 {
		r.last.Store(time.Now().UnixNano())
		r.bytes.Add(int64(n))
	}
	return n, err
}

func transferMediaOnce(ctx context.Context, client *resty.Client, url, path string, idle time.Duration) error {
	reqCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var last, received, total atomic.Int64
	last.Store(time.Now().UnixNano())
	interval := idle / 4
	if interval > time.Second {
		interval = time.Second
	}
	done := make(chan struct{})
	defer func() { cancel(nil); <-done }()
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		previousTime := time.Now()
		var previousBytes int64
		for {
			select {
			case <-reqCtx.Done():
				return
			case now := <-ticker.C:
				if now.Sub(time.Unix(0, last.Load())) >= idle {
					log.Warnf("连续 %s 无数据，取消本次下载 %s", idle, transferDisplayName(path))
					cancel(errMediaIdle)
					return
				}
				if elapsed := now.Sub(previousTime); elapsed >= 30*time.Second {
					n := received.Load()
					log.Infof("下载 %s  %s", transferProgress(n, total.Load(), float64(n-previousBytes)/elapsed.Seconds()), transferDisplayName(path))
					previousTime, previousBytes = now, n
				}
			}
		}
	}()

	// 流式读取，避免并发视频下载把整个响应体堆在内存中。
	resp, err := client.R().SetContext(reqCtx).SetQueryParam("name", "4096x4096").SetDoNotParseResponse(true).Get(url)
	if resp != nil && resp.RawBody() != nil {
		defer resp.RawBody().Close()
	}
	if errors.Is(context.Cause(reqCtx), errMediaIdle) {
		return errMediaIdle
	}
	if err != nil {
		return err
	}
	// 流式请求不能依赖读取响应体之后的钩子，显式检查 HTTP 状态。
	if err := utils.CheckRespStatus(resp); err != nil {
		return err
	}
	if resp.RawResponse.ContentLength > 0 {
		total.Store(resp.RawResponse.ContentLength)
	}

	// 临时文件放在同一目录，隐藏且不使用最终媒体名，失败/取消时删除。
	file, err := os.CreateTemp(filepath.Dir(path), ".tmd-download-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, copyErr := io.Copy(file, transferReader{resp.RawBody(), &last, &received})
	closeErr := file.Close()
	if errors.Is(context.Cause(reqCtx), errMediaIdle) {
		return errMediaIdle
	}
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}

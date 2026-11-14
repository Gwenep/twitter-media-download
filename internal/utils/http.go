package utils

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
)

func CheckRespStatus(resp *resty.Response) error {
	if resp.StatusCode() >= 400 {
		return &HttpStatusError{
			Code:       resp.StatusCode(),
			Msg:        resp.String(),
			RetryAfter: parseRetryAfter(resp),
		}
	}
	return nil
}

// parseRetryAfter 读取服务端建议的等待时间。
// x.com 的限流会返回 429，普通 API 限流带 X-Rate-Limit-Reset，
// 边缘限流（error code: 1015）则通常没有任何速率头，只能靠调用方兜底。
func parseRetryAfter(resp *resty.Response) time.Duration {
	header := resp.Header()
	if v := strings.TrimSpace(header.Get("Retry-After")); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
		if t, err := http.ParseTime(v); err == nil {
			if d := time.Until(t); d > 0 {
				return d
			}
		}
	}
	if v := strings.TrimSpace(header.Get("X-Rate-Limit-Reset")); v != "" {
		if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
			if d := time.Until(time.Unix(secs, 0)); d > 0 {
				return d
			}
		}
	}
	return 0
}

func ParseCookie(cookie string) (map[string]string, error) {
	results := make(map[string]string)
	splited := strings.Split(cookie, ";")
	for _, item := range splited {
		if len(item) == 0 {
			continue
		}
		item = strings.TrimSpace(item)
		kv := strings.SplitN(item, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("len(kv) should be 2 but '%v'", item)
		}
		results[kv[0]] = kv[1]
	}
	return results, nil
}

type HttpStatusError struct {
	Code       int
	Msg        string
	RetryAfter time.Duration
}

func (err *HttpStatusError) Error() string {
	return fmt.Sprintf("HTTP Error: %d %s", err.Code, err.Msg)
}

func IsStatusCode(err error, code int) bool {
	// resty 可能把钩子返回的错误包装成 *resty.ResponseError，
	// 因此不能只用类型断言，否则 403/404 会被当成可重试错误。
	var e *HttpStatusError
	if errors.As(err, &e) {
		return e.Code == code
	}
	return false
}

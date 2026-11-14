package twitter

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/Gwenep/twitter-media-download/internal/utils"
)

// 429（边缘限流 error code 1015）只是临时状态，必须能被识别出来等待重试，
// 否则用户的时间线会被整批跳过。
func TestRateLimitDelay(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		limited bool
		want    time.Duration
	}{
		{
			name:    "nil",
			err:     nil,
			limited: false,
		},
		{
			name:    "429 without headers",
			err:     &utils.HttpStatusError{Code: 429, Msg: "error code: 1015"},
			limited: true,
		},
		{
			name:    "429 with retry-after",
			err:     &utils.HttpStatusError{Code: 429, RetryAfter: 30 * time.Second},
			limited: true,
			want:    30 * time.Second,
		},
		{
			name:    "wrapped 429",
			err:     &resty.ResponseError{Err: &utils.HttpStatusError{Code: 429, RetryAfter: 5 * time.Second}},
			limited: true,
			want:    5 * time.Second,
		},
		{
			name:    "api error 1015",
			err:     NewTwitterApiError(ErrRateLimited, "rate limited"),
			limited: true,
		},
		{
			name:    "plain text 1015",
			err:     fmt.Errorf("HTTP Error: 429 error code: 1015"),
			limited: true,
		},
		{
			name:    "other api error",
			err:     NewTwitterApiError(ErrTimeout, "timeout"),
			limited: false,
		},
		{
			name:    "404 is not rate limit",
			err:     &utils.HttpStatusError{Code: 404},
			limited: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			delay, limited := RateLimitDelay(c.err)
			if limited != c.limited {
				t.Fatalf("limited = %v, want %v", limited, c.limited)
			}
			if c.limited && delay != c.want {
				t.Fatalf("delay = %v, want %v", delay, c.want)
			}
		})
	}
}

// errors.As 需要能穿透 resty 的包装，否则 429 会被当成普通错误丢弃。
func TestRateLimitDelayUnwraps(t *testing.T) {
	base := &utils.HttpStatusError{Code: 429}
	wrapped := fmt.Errorf("outer: %w", base)
	if !errors.As(wrapped, new(*utils.HttpStatusError)) {
		t.Fatal("errors.As should unwrap")
	}
	if _, limited := RateLimitDelay(wrapped); !limited {
		t.Fatal("wrapped 429 should be treated as rate limit")
	}
}

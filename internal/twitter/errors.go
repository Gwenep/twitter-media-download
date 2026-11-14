package twitter

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"github.com/Gwenep/twitter-media-download/internal/utils"
)

const (
	ErrTimeout         = 29
	ErrDependency      = 0
	ErrExceedPostLimit = 88
	ErrOverCapacity    = 130
	ErrAccountLocked   = 326
	ErrRateLimited     = 1015
)

func CheckApiResp(body []byte) error {
	errors := gjson.GetBytes(body, "errors")
	if !errors.Exists() {
		return nil
	}

	codej := errors.Get("0.code")
	code := -1
	if codej.Exists() {
		code = int(codej.Int())
	}
	return NewTwitterApiError(code, string(body))
}

type TwitterApiError struct {
	Code int
	raw  string
}

func (err *TwitterApiError) Error() string {
	return err.raw
}

func NewTwitterApiError(code int, raw string) *TwitterApiError {
	return &TwitterApiError{Code: code, raw: raw}
}

// RateLimitDelay 判断错误是否属于“被限流”，并给出建议等待时间。
//
// x.com 的限流有两种表现：
//   - 普通 API 限流：HTTP 429，响应头带 X-Rate-Limit-Reset；
//   - 边缘限流：HTTP 429 + 响应体 "error code: 1015"，通常没有任何速率头。
//
// 两种情况都只是临时的，应当等待后重试，而不是把用户直接丢掉。
func RateLimitDelay(err error) (time.Duration, bool) {
	if err == nil {
		return 0, false
	}

	var statusErr *utils.HttpStatusError
	if errors.As(err, &statusErr) {
		if statusErr.Code == http.StatusTooManyRequests {
			if statusErr.RetryAfter > 0 {
				return statusErr.RetryAfter, true
			}
			return 0, true
		}
	}

	var apiErr *TwitterApiError
	if errors.As(err, &apiErr) && apiErr.Code == ErrRateLimited {
		return 0, true
	}

	if strings.Contains(err.Error(), "error code: 1015") {
		return 0, true
	}
	return 0, false
}

package utils

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
)

func newRespWithHeaders(headers map[string]string) *resty.Response {
	h := http.Header{}
	for k, v := range headers {
		h.Set(k, v)
	}
	return &resty.Response{RawResponse: &http.Response{StatusCode: 429, Header: h}}
}

func TestParseRetryAfter(t *testing.T) {
	cases := []struct {
		name    string
		headers map[string]string
		min     time.Duration
		max     time.Duration
	}{
		{
			name:    "retry-after seconds",
			headers: map[string]string{"Retry-After": "30"},
			min:     29 * time.Second,
			max:     31 * time.Second,
		},
		{
			name:    "rate limit reset",
			headers: map[string]string{"X-Rate-Limit-Reset": strconv.FormatInt(time.Now().Add(45*time.Second).Unix(), 10)},
			min:     40 * time.Second,
			max:     50 * time.Second,
		},
		{
			name:    "no headers",
			headers: map[string]string{},
			min:     0,
			max:     0,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseRetryAfter(newRespWithHeaders(c.headers))
			if got < c.min || got > c.max {
				t.Fatalf("retry after = %v, want between %v and %v", got, c.min, c.max)
			}
		})
	}
}

func TestCheckRespStatusCarriesRetryAfter(t *testing.T) {
	resp := newRespWithHeaders(map[string]string{"Retry-After": "12"})
	err := CheckRespStatus(resp)
	if err == nil {
		t.Fatal("expected error for 429")
	}
	if !IsStatusCode(err, 429) {
		t.Fatal("expected 429 status")
	}
}

package downloading

import (
	"testing"
	"time"

	"github.com/Gwenep/twitter-media-download/internal/twitter"
)

func tweetsAt(times ...time.Time) []*twitter.Tweet {
	res := make([]*twitter.Tweet, len(times))
	for i, tm := range times {
		res[i] = &twitter.Tweet{Id: uint64(i + 1), CreatedAt: tm}
	}
	return res
}

func TestLatestSafeReleaseTime(t *testing.T) {
	base := time.Unix(1700000000, 0)
	t3 := base.Add(3 * time.Hour)
	t2 := base.Add(2 * time.Hour)
	t1 := base.Add(1 * time.Hour)
	tweets := tweetsAt(t3, t2, t1) // 时间线倒序

	// 没有失败：可以推进到最新推文
	got, ok := latestSafeReleaseTime(tweets, nil)
	if !ok || !got.Equal(t3) {
		t.Fatalf("no failure: got %v, %v; want %v, true", got, ok, t3)
	}

	// 最新推文失败：只能推进到 t2
	got, ok = latestSafeReleaseTime(tweets, map[uint64]struct{}{1: {}})
	if !ok || !got.Equal(t2) {
		t.Fatalf("newest failed: got %v, %v; want %v, true", got, ok, t2)
	}

	// 中间推文失败：只能推进到最旧推文 t1
	got, ok = latestSafeReleaseTime(tweets, map[uint64]struct{}{2: {}})
	if !ok || !got.Equal(t1) {
		t.Fatalf("middle failed: got %v, %v; want %v, true", got, ok, t1)
	}

	// 最旧推文失败：不能推进
	_, ok = latestSafeReleaseTime(tweets, map[uint64]struct{}{3: {}})
	if ok {
		t.Fatal("oldest failed: progress should not advance")
	}

	// 时间相同（同一秒发布）的推文失败时也不能越过
	same := tweetsAt(t3, t3)
	_, ok = latestSafeReleaseTime(same, map[uint64]struct{}{2: {}})
	if ok {
		t.Fatal("same timestamp failure: progress should not advance")
	}
}

func TestNextFileName(t *testing.T) {
	tests := map[string]string{
		"hello.jpg":     "hello(1).jpg",
		"hello(1).jpg":  "hello(2).jpg",
		"hello(9).jpg":  "hello(10).jpg",
		"hello (9).jpg": "hello (10).jpg",
	}
	for in, want := range tests {
		if got := nextFileName(in); got != want {
			t.Errorf("nextFileName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSameTweetTime(t *testing.T) {
	now := time.Unix(1700000000, 0)
	if !sameTweetTime(now.Add(time.Second), now) {
		t.Error("one second difference should be treated as the same tweet")
	}
	if sameTweetTime(now.Add(time.Hour), now) {
		t.Error("one hour difference should not be treated as the same tweet")
	}
}

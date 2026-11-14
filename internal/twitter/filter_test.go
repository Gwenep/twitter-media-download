package twitter

import (
	"testing"
	"time"
)

func tweetAt(id uint64, at time.Time) *Tweet {
	return &Tweet{Id: id, CreatedAt: at}
}

// 置顶推文（较旧）混在页面中时，不能因此判定已经翻到旧内容区。
func TestFilterTweetsByTimeRangePinnedOldTweet(t *testing.T) {
	min := time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)
	pinned := tweetAt(1, min.Add(-72*time.Hour))
	newer := tweetAt(2, min.Add(2*time.Hour))
	newest := tweetAt(3, min.Add(3*time.Hour))

	reachedMin, res := filterTweetsByTimeRange([]*Tweet{pinned, newer, newest}, &min, nil)
	if reachedMin {
		t.Fatal("page still contains tweets >= min, must not stop paging")
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 tweets in range, got %d", len(res))
	}
	if res[0].Id != 2 || res[1].Id != 3 {
		t.Fatalf("unexpected tweets: %d, %d", res[0].Id, res[1].Id)
	}
}

// 整页推文都早于下界时，才允许停止翻页。
func TestFilterTweetsByTimeRangeAllOld(t *testing.T) {
	min := time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)
	old1 := tweetAt(1, min.Add(-2*time.Hour))
	old2 := tweetAt(2, min.Add(-time.Hour))

	reachedMin, res := filterTweetsByTimeRange([]*Tweet{old1, old2}, &min, nil)
	if !reachedMin {
		t.Fatal("all tweets are older than min, should stop paging")
	}
	if len(res) != 0 {
		t.Fatalf("expected no tweets, got %d", len(res))
	}
}

// 下界是闭区间：时间恰好等于 min 的推文也必须保留。
func TestFilterTweetsByTimeRangeInclusiveMin(t *testing.T) {
	min := time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)
	equal := tweetAt(1, min)
	older := tweetAt(2, min.Add(-time.Second))

	reachedMin, res := filterTweetsByTimeRange([]*Tweet{equal, older}, &min, nil)
	if reachedMin {
		t.Fatal("tweet exactly at min must count as in range")
	}
	if len(res) != 1 || res[0].Id != 1 {
		t.Fatalf("expected only tweet at min, got %+v", res)
	}
}

// 上界同样按闭区间处理，用于限制回溯范围。
func TestFilterTweetsByTimeRangeMax(t *testing.T) {
	min := time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)
	max := min.Add(time.Hour)
	tooNew := tweetAt(1, max.Add(time.Second))
	inRange := tweetAt(2, max)

	reachedMin, res := filterTweetsByTimeRange([]*Tweet{tooNew, inRange}, &min, &max)
	if reachedMin {
		t.Fatal("tweets >= min exist, must not stop paging")
	}
	if len(res) != 1 || res[0].Id != 2 {
		t.Fatalf("expected only tweet within max, got %+v", res)
	}
}

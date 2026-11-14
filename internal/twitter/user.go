package twitter

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/tidwall/gjson"
	"github.com/Gwenep/twitter-media-download/internal/utils"
)

type FollowState int

// timelinePageInterval 相邻两次时间线请求之间的最小间隔。
// x.com 对 UserMedia 这类 GraphQL 接口有边缘限流（429 / error code 1015），
// 短时间内连续翻页很容易触发，这里做一点节流。
var timelinePageInterval = 1500 * time.Millisecond

const (
	FS_UNFOLLOW FollowState = iota
	FS_FOLLOWING
	FS_REQUESTED
)

type User struct {
	Id           uint64
	Name         string
	ScreenName   string
	IsProtected  bool
	FriendsCount int
	Followstate  FollowState
	MediaCount   int
	Muting       bool
	Blocking     bool
}

func GetUserById(ctx context.Context, client *resty.Client, id uint64) (*User, error) {
	api := userByRestId{id}
	getUrl := makeUrl(&api)
	r, err := getUser(ctx, client, getUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to get user [%d]: %v", id, err)
	}
	return r, err
}

func GetUserByScreenName(ctx context.Context, client *resty.Client, screenName string) (*User, error) {
	u := makeUrl(&userByScreenName{screenName: screenName})
	r, err := getUser(ctx, client, u)
	if err != nil {
		return nil, fmt.Errorf("failed to get user [%s]: %v", screenName, err)
	}
	return r, err
}

func getUser(ctx context.Context, client *resty.Client, url string) (*User, error) {
	resp, err := client.R().SetContext(ctx).Get(url)
	if err != nil {
		return nil, err
	}
	return parseRespJson(resp.Body())
}

func parseUserResults(user_results *gjson.Result) (*User, error) {
	result := user_results.Get("result")
	if result.Get("__typename").String() == "UserUnavailable" {
		return nil, fmt.Errorf("user unavaiable")
	}
	legacy := result.Get("legacy")

	restId := result.Get("rest_id")
	friends_count := legacy.Get("friends_count")
	name := legacy.Get("name")
	screen_name := legacy.Get("screen_name")
	protected := legacy.Get("protected").Exists() && legacy.Get("protected").Bool()
	media_count := legacy.Get("media_count")
	muting := legacy.Get("muting")
	blocking := legacy.Get("blocking")

	usr := User{}
	if foll := legacy.Get("following"); foll.Exists() {
		if foll.Bool() {
			usr.Followstate = FS_FOLLOWING
		} else {
			usr.Followstate = FS_UNFOLLOW
		}
	} else if legacy.Get("follow_request_sent").Exists() {
		usr.Followstate = FS_REQUESTED
	} else {
		usr.Followstate = FS_UNFOLLOW
	}

	usr.FriendsCount = int(friends_count.Int())
	usr.Id = restId.Uint()
	usr.IsProtected = protected
	usr.Name = name.String()
	usr.ScreenName = screen_name.String()
	usr.MediaCount = int(media_count.Int())
	usr.Muting = muting.Exists() && muting.Bool()
	usr.Blocking = blocking.Exists() && blocking.Bool()
	return &usr, nil
}

func parseRespJson(resp []byte) (*User, error) {
	user := gjson.GetBytes(resp, "data.user")
	if !user.Exists() {
		return nil, fmt.Errorf("user does not exist")
	}
	return parseUserResults(&user)
}

func (u *User) IsVisiable() bool {
	return u.Followstate == FS_FOLLOWING || !u.IsProtected
}

func itemContentsToTweets(itemContents []gjson.Result) []*Tweet {
	res := make([]*Tweet, 0, len(itemContents))
	for _, itemContent := range itemContents {
		tweetResults := getResults(itemContent, timelineTweet)
		if tw := parseTweetResults(&tweetResults); tw != nil {
			res = append(res, tw)
		}
	}
	return res
}

func (u *User) getMediasOnePage(ctx context.Context, api *userMedia, client *resty.Client) ([]*Tweet, string, error) {
	if !u.IsVisiable() {
		return nil, "", nil
	}

	itemContents, next, err := getTimelineItemContents(ctx, api, client, "data.user.result.timeline_v2.timeline.instructions")
	return itemContentsToTweets(itemContents), next, err
}

// filterTweetsByTimeRange 筛选一页中落在时间范围内的推文。
//
// min 为闭区间下界：时间恰好等于 min 的推文也会保留。
// 这样即使多条推文发布于同一秒，也不会因为边界比较而漏掉。
//
// reachedMin 表示这一页已经没有任何 >= min 的推文，可以停止翻页。
// 该判断基于整页而不是“第一条早于 min 的推文”，因此置顶推文（时间较旧）
// 混在页面中时不会导致提前停止翻页。
func filterTweetsByTimeRange(tweets []*Tweet, min *time.Time, max *time.Time) (reachedMin bool, res []*Tweet) {
	reachedMin = true
	for _, tw := range tweets {
		if min != nil && !min.IsZero() && tw.CreatedAt.Before(*min) {
			continue
		}
		// 页内存在达到下界的推文，说明还没翻到旧内容区
		reachedMin = false

		if max != nil && !max.IsZero() && tw.CreatedAt.After(*max) {
			continue
		}
		res = append(res, tw)
	}
	return reachedMin, res
}

func (u *User) GetMeidas(ctx context.Context, client *resty.Client, timeRange *utils.TimeRange) ([]*Tweet, error) {
	if !u.IsVisiable() {
		return nil, nil
	}

	api := userMedia{}
	api.count = 100
	api.cursor = ""
	api.userId = u.Id

	results := make([]*Tweet, 0)

	var minTime *time.Time
	var maxTime *time.Time

	if timeRange != nil {
		minTime = &timeRange.Min
		maxTime = &timeRange.Max
	}

	prevCursor := ""
	emptyPages := 0
	firstPage := true
	for {
		// 页与页之间稍作停顿。旧版本通常只请求 1 页就停止，新版为了兼容
		// 置顶/乱序推文会多翻一页，连续请求太密容易触发 x.com 的边缘限流
		// （HTTP 429 + error code: 1015）。
		if !firstPage {
			select {
			case <-time.After(timelinePageInterval):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		firstPage = false

		currentTweets, next, err := u.getMediasOnePage(ctx, &api, client)
		if err != nil {
			return nil, err
		}

		if len(currentTweets) == 0 {
			// 空页但仍有新 cursor 时不能直接结束：时间线中可能混有非推文模块，
			// 直接退出会漏掉后面的内容。但只多探一页，连续空页就没必要继续翻，
			// 否则白白消耗请求次数，容易触发限流。
			if next == "" || next == prevCursor || next == api.cursor || emptyPages >= 1 {
				break
			}
			emptyPages++
			prevCursor = api.cursor
			api.SetCursor(next)
			continue
		}
		emptyPages = 0

		if timeRange == nil {
			results = append(results, currentTweets...)
		} else {
			reachedMin, page := filterTweetsByTimeRange(currentTweets, minTime, maxTime)
			results = append(results, page...)
			if reachedMin {
				break
			}
		}

		// cursor 没有前进说明已经到末尾，避免死循环
		if next == "" || next == prevCursor || next == api.cursor {
			break
		}
		prevCursor = api.cursor
		api.SetCursor(next)
	}

	// 时间线可能包含置顶/重复推文，统一按时间倒序并去重
	sort.SliceStable(results, func(i, j int) bool {
		return results[i].CreatedAt.After(results[j].CreatedAt)
	})
	if len(results) > 1 {
		seen := make(map[uint64]struct{}, len(results))
		deduped := make([]*Tweet, 0, len(results))
		for _, tw := range results {
			if _, ok := seen[tw.Id]; ok {
				continue
			}
			seen[tw.Id] = struct{}{}
			deduped = append(deduped, tw)
		}
		results = deduped
	}
	return results, nil
}

func (u *User) Title() string {
	return fmt.Sprintf("%s(%s)", u.Name, u.ScreenName)
}

func (u *User) Following() UserFollowing {
	return UserFollowing{u}
}

func FollowUser(ctx context.Context, client *resty.Client, user *User) error {
	url := "https://x.com/i/api/1.1/friendships/create.json"
	_, err := client.R().SetFormData(map[string]string{
		"user_id": fmt.Sprintf("%d", user.Id),
		// "skip_status":                       1,
		// "include_profile_interstitial_type": 1,
		// "include_blocking":                  1,
		// "include_blocked_by":                1,
		// "include_followed_by":               1,
		// "include_want_retweets":             1,
		// "include_mute_edge":                 1,
		// "include_can_dm":                    1,
		// "include_can_media_tag":             1,
		// "include_ext_is_blue_verified":      1,
		// "include_ext_verified_type":         1,
		// "include_ext_profile_image_shape":   1,
	}).SetContext(ctx).Post(url)
	return err
}

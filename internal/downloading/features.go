package downloading

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/gookit/color"
	"github.com/jmoiron/sqlx"
	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"
	"github.com/Gwenep/twitter-media-download/internal/database"
	"github.com/Gwenep/twitter-media-download/internal/twitter"
	"github.com/Gwenep/twitter-media-download/internal/utils"
)

type PackgedTweet interface {
	GetTweet() *twitter.Tweet
	GetPath() string
	// GetEntity 返回推文所属的用户实体；TweetInDir 没有实体时返回 nil
	GetEntity() *UserEntity
}

type TweetInDir struct {
	tweet *twitter.Tweet
	path  string
}

func (pt TweetInDir) GetTweet() *twitter.Tweet {
	return pt.tweet
}

func (pt TweetInDir) GetPath() string {
	return pt.path
}

func (pt TweetInDir) GetEntity() *UserEntity {
	return nil
}

// 保护文件名分配和 media_files 预定，避免并发下载到同一个文件名
var mutex sync.Mutex

// 媒体请求返回 403/404/410 时重试也没有意义（被删除、DMCA 等），
// 视为“永久跳过”，但不能因此放弃同一条推文里的其它媒体。
func isPermanentMediaError(err error) bool {
	return utils.IsStatusCode(err, 403) ||
		utils.IsStatusCode(err, 404) ||
		utils.IsStatusCode(err, 410)
}

// sameTweetTime 判断文件修改时间是否与推文时间一致。
// 旧版本下载的文件会把 mtime 设置为推文发布时间，可借此识别旧文件，
// 升级后第一次运行不会把已经下载过的内容再下载一遍。
func sameTweetTime(modTime, tweetTime time.Time) bool {
	diff := modTime.Sub(tweetTime)
	if diff < 0 {
		diff = -diff
	}
	return diff <= 2*time.Second
}

// nextFileName 在 name 的基础上生成下一个候选文件名。
// 命名规则与 utils.UniquePath 保持一致（如 text.jpg -> text(1).jpg），
// 这样旧版本下载的文件也能被正确识别并复用。
func nextFileName(name string) string {
	ext := filepath.Ext(name)
	stem := strings.TrimSuffix(name, ext)
	if strings.HasSuffix(stem, ")") {
		if left := strings.LastIndex(stem, "("); left != -1 {
			if n, err := strconv.Atoi(stem[left+1 : len(stem)-1]); err == nil {
				return fmt.Sprintf("%s(%d)%s", stem[:left], n+1, ext)
			}
		}
	}
	return fmt.Sprintf("%s(1)%s", stem, ext)
}

// prepareMediaFile 决定某个媒体地址应当保存为哪个文件名。
//
// 返回 (文件名, 是否已经存在, 本次是否预定了记录, 错误)
//
//	已存在：直接跳过下载
//	预定：先在 media_files 中占住文件名，再下载，防止并发覆盖
func prepareMediaFile(db *sqlx.DB, entity *UserEntity, dir string, tweet *twitter.Tweet, url string, preferred string) (string, bool, bool, error) {
	mutex.Lock()
	defer mutex.Unlock()

	if db != nil && entity != nil {
		recorded, err := database.GetMediaFile(db, tweet.Id, url)
		if err != nil {
			return "", false, false, err
		}
		if recorded != nil {
			path := filepath.Join(dir, recorded.FileName)
			exist, err := utils.PathExists(path)
			if err != nil {
				return "", false, false, err
			}
			// 已有记录：文件存在则跳过；不存在则沿用原来的文件名重新下载
			return recorded.FileName, exist, false, nil
		}
	}

	// 兼容旧版本下载的文件：仅当文件名未被占用且修改时间与推文时间吻合时才认领
	legacyPath := filepath.Join(dir, preferred)
	if exist, err := utils.PathExists(legacyPath); err != nil {
		return "", false, false, err
	} else if exist {
		claimed := false
		if db != nil && entity != nil {
			if mf, err := database.GetMediaFileByName(db, int32(entity.Id()), preferred); err != nil {
				return "", false, false, err
			} else if mf != nil {
				claimed = true
			}
		}
		if !claimed {
			matched := false
			if info, err := os.Stat(legacyPath); err == nil {
				// 旧版本下载的文件会把 mtime 写成推文发布时间
				matched = sameTweetTime(info.ModTime(), tweet.CreatedAt)
				if !matched {
					// mtime 被系统工具/同步软件改过时，用目录索引（.tmd_media.json）
					// 里的文件名 + 大小认领：大小完全一致才认为是当初下载的那个文件
					if entry, ok := database.LookupManifestFile(dir, preferred); ok &&
						entry.Size == info.Size() && info.Size() > 0 {
						matched = true
					}
				}
			}
			if matched {
				if db != nil && entity != nil {
					if err := database.CreateMediaFile(db, &database.MediaFile{
						TweetId:      tweet.Id,
						Url:          url,
						FileName:     preferred,
						UserEntityId: int32(entity.Id()),
					}); err != nil {
						return "", false, false, err
					}
				}
				return preferred, true, false, nil
			}
		}
	}

	// 分配一个未被文件系统或其他记录占用的文件名
	name := preferred
	for {
		full := filepath.Join(dir, name)
		exist, err := utils.PathExists(full)
		if err != nil {
			return "", false, false, err
		}

		claimed := false
		if db != nil && entity != nil {
			mf, err := database.GetMediaFileByName(db, int32(entity.Id()), name)
			if err != nil {
				return "", false, false, err
			}
			claimed = mf != nil
		}
		if !exist && !claimed {
			break
		}

		if exist {
			unique, err := utils.UniquePath(full)
			if err != nil {
				return "", false, false, err
			}
			name = filepath.Base(unique)
		} else {
			// 文件不存在但名字已被别的推文预定
			name = nextFileName(name)
		}
	}

	reserved := false
	if db != nil && entity != nil {
		if err := database.CreateMediaFile(db, &database.MediaFile{
			TweetId:      tweet.Id,
			Url:          url,
			FileName:     name,
			UserEntityId: int32(entity.Id()),
		}); err != nil {
			return "", false, false, err
		}
		reserved = true
	}
	return name, false, reserved, nil
}

// downloadTweetMedia 下载一条推文中的所有媒体。
//
// 与旧实现不同：
//   - 一个媒体失败不会阻止同一条推文里的其它媒体继续下载
//   - 已下载的文件通过 media_files 精确识别，重试不会产生重复文件
//   - 403/404/410 只跳过对应的媒体，并把它视为已处理，不会被无限重试
func downloadTweetMedia(ctx context.Context, client *resty.Client, db *sqlx.DB, entity *UserEntity, dir string, tweet *twitter.Tweet) error {
	text := utils.WinFileName(tweet.Text)
	downloaded := 0
	var firstTransient error
	extCount := make(map[string]int)

	for _, u := range tweet.Urls {
		if u == "" {
			continue
		}

		ext, err := utils.GetExtFromUrl(u)
		if err != nil || ext == "" {
			log.WithFields(log.Fields{
				"user": tweet.Creator.Title(),
				"url":  u,
			}).Warnln("failed to get file extension from url, skipped")
			continue
		}
		ext = strings.ToLower(ext)

		index := extCount[ext]
		extCount[ext] = index + 1
		preferred := text + ext
		if index > 0 {
			preferred = fmt.Sprintf("%s(%d)%s", text, index, ext)
		}

		name, exist, reserved, err := prepareMediaFile(db, entity, dir, tweet, u, preferred)
		if err != nil {
			if firstTransient == nil {
				firstTransient = err
			}
			continue
		}
		if exist {
			log.Debugf("media already exists, skipped: %s", filepath.Join(dir, name))
			continue
		}

		path := filepath.Join(dir, name)
		resp, err := client.R().SetContext(ctx).SetQueryParam("name", "4096x4096").Get(u)
		if err != nil {
			if reserved && db != nil {
				_ = database.DeleteMediaFile(db, tweet.Id, u)
			}
			if isPermanentMediaError(err) {
				log.WithFields(log.Fields{
					"user": tweet.Creator.Title(),
					"url":  u,
				}).Warnln("media is unavailable (403/404/410), skipped:", err)
				continue
			}
			if firstTransient == nil {
				firstTransient = err
			}
			continue
		}

		file, err := os.Create(path)
		if err != nil {
			if reserved && db != nil {
				_ = database.DeleteMediaFile(db, tweet.Id, u)
			}
			if firstTransient == nil {
				firstTransient = err
			}
			continue
		}

		_, err = file.Write(resp.Body())
		closeErr := file.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			if reserved && db != nil {
				_ = database.DeleteMediaFile(db, tweet.Id, u)
			}
			_ = os.Remove(path)
			if firstTransient == nil {
				firstTransient = err
			}
			continue
		}

		// 保留推文发布日期，同时作为旧文件识别依据
		_ = os.Chtimes(path, time.Time{}, tweet.CreatedAt)
		downloaded++
	}

	if downloaded > 0 {
		fmt.Printf("%s %s\n", color.FgLightMagenta.Render("["+tweet.Creator.Title()+"]"), text)
	}
	return firstTransient
}

var MaxDownloadRoutine int

// map[user_id]*UserEntity 记录本次程序运行已同步过的用户
var syncedUsers sync.Map

func init() {
	MaxDownloadRoutine = min(100, runtime.GOMAXPROCS(0)*10)
}

type workerConfig struct {
	ctx    context.Context
	wg     *sync.WaitGroup
	cancel context.CancelCauseFunc
}

// 负责下载推文，保证 tweet chan 内的推文要么下载成功，要么推送至 error chan
func tweetDownloader(client *resty.Client, db *sqlx.DB, config *workerConfig, errch chan<- PackgedTweet, twech <-chan PackgedTweet) {
	var pt PackgedTweet
	var ok bool

	defer config.wg.Done()
	defer func() {
		if p := recover(); p != nil {
			config.cancel(fmt.Errorf("%v", p)) // panic 取消上下文，防止生产者死锁
			log.WithField("worker", "downloading").Errorln("panic:", p)

			if pt != nil {
				errch <- pt // push 正下载的推文
			}
			// 确保只有1个协程的情况下，未能下载完毕的推文仍然会全部推送到 errch
			for pt := range twech {
				errch <- pt
			}
		}
	}()

	for {
		select {
		case pt, ok = <-twech:
			if !ok {
				return
			}
		case <-config.ctx.Done():
			for pt := range twech {
				errch <- pt
			}
			return
		}

		path := pt.GetPath()
		if path == "" {
			errch <- pt
			continue
		}
		err := downloadTweetMedia(config.ctx, client, db, pt.GetEntity(), path, pt.GetTweet())
		// 403/404/410: 被删除或 DMCA，重试无意义
		if err != nil && !isPermanentMediaError(err) {
			errch <- pt
		}

		// cancel context and exit if no disk space
		if errors.Is(err, syscall.ENOSPC) {
			config.cancel(err)
		}
	}
}

// 批量下载推文并返回下载失败的推文，可以保证推文被成功下载或被返回
func BatchDownloadTweet(ctx context.Context, client *resty.Client, db *sqlx.DB, pts ...PackgedTweet) []PackgedTweet {
	if len(pts) == 0 {
		return nil
	}

	ctx, cancel := context.WithCancelCause(ctx)

	var errChan = make(chan PackgedTweet)
	var tweetChan = make(chan PackgedTweet, len(pts))
	var wg sync.WaitGroup // number of working goroutines
	var numRoutine = min(len(pts), MaxDownloadRoutine)

	for _, pt := range pts {
		tweetChan <- pt
	}
	close(tweetChan)

	config := workerConfig{
		ctx:    ctx,
		cancel: cancel,
		wg:     &wg,
	}
	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go tweetDownloader(client, db, &config, errChan, tweetChan)
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	errors := []PackgedTweet{}
	for pt := range errChan {
		errors = append(errors, pt)
	}
	return errors
}

// 更新数据库中对用户的记录
func syncUser(db *sqlx.DB, user *twitter.User) error {
	renamed := false
	isNew := false
	usrdb, err := database.GetUserById(db, user.Id)
	if err != nil {
		return err
	}

	if usrdb == nil {
		isNew = true
		usrdb = &database.User{}
		usrdb.Id = user.Id
	} else {
		renamed = usrdb.Name != user.Name || usrdb.ScreenName != user.ScreenName
	}

	usrdb.FriendsCount = user.FriendsCount
	usrdb.IsProtected = user.IsProtected
	usrdb.Name = user.Name
	usrdb.ScreenName = user.ScreenName

	if isNew {
		err = database.CreateUser(db, usrdb)
	} else {
		err = database.UpdateUser(db, usrdb)
	}
	if err != nil {
		return err
	}
	if renamed || isNew {
		err = database.RecordUserPreviousName(db, user.Id, user.Name, user.ScreenName)
	}
	return err
}

func failedTweetIDs(fails []PackgedTweet) map[uint64]struct{} {
	ids := make(map[uint64]struct{}, len(fails))
	for _, pt := range fails {
		if pt == nil || pt.GetTweet() == nil {
			continue
		}
		ids[pt.GetTweet().Id] = struct{}{}
	}
	return ids
}

// latestSafeReleaseTime 计算本次可以安全推进的“最后下载时间”。
//
// 如果没有任何失败，可以推进到最新推文；
// 如果有失败，只能推进到“比最旧失败推文更旧的最新一条成功推文”，
// 这样下一次拉取时间线时一定会包含失败的推文。
// 即使进程被强制结束、错误记录来不及写入磁盘，失败内容也不会被永久跳过。
func latestSafeReleaseTime(tweets []*twitter.Tweet, failed map[uint64]struct{}) (time.Time, bool) {
	if len(tweets) == 0 {
		return time.Time{}, false
	}

	sorted := make([]*twitter.Tweet, len(tweets))
	copy(sorted, tweets)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].CreatedAt.After(sorted[j].CreatedAt)
	})

	var oldestFail *time.Time
	for _, tw := range sorted {
		if _, ok := failed[tw.Id]; !ok {
			continue
		}
		if oldestFail == nil || tw.CreatedAt.Before(*oldestFail) {
			t := tw.CreatedAt
			oldestFail = &t
		}
	}
	if oldestFail == nil {
		return sorted[0].CreatedAt, true
	}

	for _, tw := range sorted {
		if tw.CreatedAt.Before(*oldestFail) {
			return tw.CreatedAt, true
		}
	}
	return time.Time{}, false
}

func DownloadUser(ctx context.Context, db *sqlx.DB, client *resty.Client, user *twitter.User, dir string) ([]PackgedTweet, error) {
	if user.Blocking || user.Muting {
		return nil, nil
	}

	_, loaded := syncedUsers.Load(user.Id)
	if loaded {
		log.WithField("user", user.Title()).Debugln("skiped downloaded user")
		return nil, nil
	}
	entity, err := syncUserAndEntity(db, user, dir)
	if err != nil {
		return nil, err
	}

	syncedUsers.Store(user.Id, entity)
	tweets, err := user.GetMeidas(ctx, client, &utils.TimeRange{Min: entity.LatestReleaseTime()})
	if err != nil || len(tweets) == 0 {
		return nil, err
	}

	// 打包推文
	pts := make([]PackgedTweet, 0, len(tweets))
	for _, tw := range tweets {
		pts = append(pts, TweetInEntity{Tweet: tw, Entity: entity})
	}

	fails := BatchDownloadTweet(ctx, client, db, pts...)
	failed := failedTweetIDs(fails)
	if newTime, ok := latestSafeReleaseTime(tweets, failed); ok {
		if len(failed) == 0 {
			err = database.UpdateUserEntityTweetStat(db, entity.Id(), newTime, user.MediaCount)
		} else {
			err = database.SetUserEntityLatestReleaseTime(db, entity.Id(), newTime)
		}
		if err != nil {
			return fails, err
		}
	}
	return fails, nil
}

// safeUserTitle 处理不同用户恰好同名（或旧数据损坏）的情况，
// 避免把两个用户写到同一个文件夹。
func safeUserTitle(dir string, user *twitter.User) string {
	title := utils.WinFileName(user.Title())
	target := filepath.Join(dir, title)
	if marker, _ := database.ReadUserMarker(target); marker != nil && marker.UserId != user.Id {
		title = fmt.Sprintf("%s [%d]", title, user.Id)
	}
	return title
}

func syncUserAndEntity(db *sqlx.DB, user *twitter.User, dir string) (*UserEntity, error) {
	if err := syncUser(db, user); err != nil {
		return nil, err
	}
	expectedTitle := safeUserTitle(dir, user)

	entity, err := NewUserEntity(db, user.Id, dir)
	if err != nil {
		return nil, err
	}

	// 旧版本可能把当前用户的记录错误关联到另一个用户的文件夹（同名匹配未校验 user_id）。
	// 这种情况下只修正数据库里的名称，绝不 Rename 移动别人的文件夹。
	if cur, err := entity.Path(); err == nil {
		if marker, _ := database.ReadUserMarker(cur); marker != nil && marker.UserId != user.Id {
			if err := entity.RepairName(expectedTitle); err != nil {
				return nil, err
			}
		}
	}

	if err = syncPath(entity, expectedTitle); err != nil {
		return nil, err
	}

	// 最后一道防线：文件夹若明确属于其他用户，绝不写入
	path, err := entity.Path()
	if err != nil {
		return nil, err
	}
	if marker, _ := database.ReadUserMarker(path); marker != nil && marker.UserId != user.Id {
		return nil, fmt.Errorf("folder %s belongs to user %d", path, marker.UserId)
	}
	if err := database.WriteUserMarker(path, user.Id, user.Name, user.ScreenName); err != nil {
		return nil, err
	}
	return entity, nil
}

type TweetInEntity struct {
	Tweet  *twitter.Tweet
	Entity *UserEntity
}

func (pt TweetInEntity) GetTweet() *twitter.Tweet {
	return pt.Tweet
}

func (pt TweetInEntity) GetPath() string {
	path, err := pt.Entity.Path()
	if err != nil {
		return ""
	}
	return path
}

func (pt TweetInEntity) GetEntity() *UserEntity {
	return pt.Entity
}

const userTweetRateLimit = 500
const userTweetMaxConcurrent = 100 // avoid DownstreamOverCapacityError

// var syncedListUsers = make(map[uint64]map[int64]struct{})
var syncedListUsers sync.Map //leid -> uid -> struct{}

// 需要请求多少次时间线才能获取完毕用户的推文？
func calcUserDepth(exist int, total int) int {
	if exist >= total {
		return 1
	}

	miss := total - exist
	depth := miss / twitter.AvgTweetsPerPage
	if miss%twitter.AvgTweetsPerPage != 0 {
		depth++
	}
	if exist == 0 {
		depth++ //对于新用户，需要多获取一个空页
	}
	return depth
}

type userInLstEntity struct {
	user *twitter.User
	leid *int
}

// 本次运行中成功获取到的时间线，下载全部结束后再根据下载结果更新进度
type userFetchStat struct {
	entity     *UserEntity
	mediaCount int
	tweets     []*twitter.Tweet
}

func finalizeUserFetchStats(db *sqlx.DB, stats map[int]*userFetchStat, failed map[uint64]struct{}) error {
	for _, st := range stats {
		newTime, ok := latestSafeReleaseTime(st.tweets, failed)
		if !ok {
			continue
		}

		hasFail := false
		for _, tw := range st.tweets {
			if _, exists := failed[tw.Id]; exists {
				hasFail = true
				break
			}
		}

		var err error
		if hasFail {
			// 有失败时只推进到安全位置，媒体数量保持不变
			err = database.SetUserEntityLatestReleaseTime(db, st.entity.Id(), newTime)
		} else {
			err = database.UpdateUserEntityTweetStat(db, st.entity.Id(), newTime, st.mediaCount)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func shouldIngoreUser(user *twitter.User) bool {
	return user.Blocking || user.Muting
}

func BatchUserDownload(ctx context.Context, client *resty.Client, db *sqlx.DB, users []userInLstEntity, dir string, autoFollow bool, additional []*resty.Client) ([]*TweetInEntity, error) {
	if len(users) == 0 {
		return nil, nil
	}

	uidToUser := make(map[uint64]*twitter.User)
	for _, u := range users {
		uidToUser[u.user.Id] = u.user
	}

	// channels
	tweetChan := make(chan PackgedTweet, MaxDownloadRoutine)
	errChan := make(chan PackgedTweet)
	// WG
	prodwg := sync.WaitGroup{}
	conswg := sync.WaitGroup{}
	// ctx
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	// logger
	updaterLogger := log.WithField("worker", "updating")
	getterLogger := log.WithField("worker", "getting")

	panicHandler := func() {
		if r := recover(); r != nil {
			cancel(fmt.Errorf("%v", r))
			buf := make([]byte, 1<<16)
			n := runtime.Stack(buf, false)
			fmt.Printf("Recovered from panic: %v\n%s\n", r, buf[:n])
		}
	}

	missingTweets := 0
	depthByEntity := make(map[*UserEntity]int)
	// 被限流的用户重试次数；限流是临时的，不能像普通错误那样直接放弃
	rateLimitRetries := make(map[*UserEntity]int)
	var rateLimitMtx sync.Mutex
	rateLimitedUsers := make(map[string]struct{})
	// 限流最多重试几次、每次至少等多久（服务端没给 Retry-After 时使用）
	const maxRateLimitRetry = 5
	const defaultRateLimitWait = 60 * time.Second
	const maxRateLimitWait = 15 * time.Minute
	// 大顶堆，以用户深度
	userEntityHeap := utils.NewHeap(func(lhs, rhs *UserEntity) bool {
		luser, ruser := uidToUser[lhs.Uid()], uidToUser[rhs.Uid()]
		lOnlyMater := luser.IsProtected && luser.Followstate == twitter.FS_FOLLOWING
		rOnlyMaster := ruser.IsProtected && ruser.Followstate == twitter.FS_FOLLOWING

		if lOnlyMater == rOnlyMaster {
			return depthByEntity[lhs] > depthByEntity[rhs]
		}
		return lOnlyMater // 优先让 master 获取只有他能看到的
	})

	start := time.Now()
	deepest := 0

	// pre-process
	func() {
		defer panicHandler()
		log.Infoln("start pre processing users")

		for _, userInLST := range users {
			var pathEntity *UserEntity
			var err error
			user := userInLST.user
			leid := userInLST.leid

			if shouldIngoreUser(user) {
				continue
			}

			pe, loaded := syncedUsers.Load(user.Id)
			if !loaded {
				pathEntity, err = syncUserAndEntity(db, user, dir)
				if err != nil {
					updaterLogger.WithField("user", user.Title()).Warnln("failed to update user or entity", err)
					continue
				}
				syncedUsers.Store(user.Id, pathEntity)

				// 同步所有现存的指向此用户的符号链接
				upath, _ := pathEntity.Path()
				linkds, err := database.GetUserLinks(db, user.Id)
				if err != nil {
					updaterLogger.WithField("user", user.Title()).Warnln("failed to get links to user:", err)
				}
				for _, linkd := range linkds {
					if err = updateUserLink(linkd, db, upath); err != nil {
						updaterLogger.WithField("user", user.Title()).Warnln("failed to update link:", err)
					}
					sl, _ := syncedListUsers.LoadOrStore(int(linkd.ParentLstEntityId), &sync.Map{})
					syncedList := sl.(*sync.Map)
					syncedList.Store(user.Id, struct{}{})
				}

				// 计算深度
				if user.MediaCount != 0 && user.IsVisiable() {
					missingTweets += max(0, user.MediaCount-int(pathEntity.record.MediaCount.Int32))
					depthByEntity[pathEntity] = calcUserDepth(int(pathEntity.record.MediaCount.Int32), user.MediaCount)
					userEntityHeap.Push(pathEntity)
					deepest = max(deepest, depthByEntity[pathEntity])
				}

				// 自动关注
				if user.IsProtected && user.Followstate == twitter.FS_UNFOLLOW && autoFollow {
					if err := twitter.FollowUser(ctx, client, user); err != nil {
						log.WithField("user", user.Title()).Warnln("failed to follow user:", err)
					} else {
						log.WithField("user", user.Title()).Debugln("follow request has been sent")
					}
				}
			} else {
				pathEntity = pe.(*UserEntity)
			}

			// 即便同步一个用户时也同步了所有指向此用户的链接，
			// 但此用户仍可能会是一个新的 “列表-用户”，所以判断此用户链接是否同步过，
			// 如果否，那么创建一个属于此列表的用户链接
			if leid == nil {
				continue
			}
			sl, _ := syncedListUsers.LoadOrStore(*leid, &sync.Map{})
			syncedList := sl.(*sync.Map)
			_, loaded = syncedList.LoadOrStore(user.Id, struct{}{})
			if loaded {
				continue
			}

			// 为当前列表的新用户创建符号链接
			upath, _ := pathEntity.Path()
			var linkname = pathEntity.Name()

			curlink := &database.UserLink{}
			curlink.Name = linkname
			curlink.ParentLstEntityId = int32(*leid)
			curlink.Uid = user.Id

			linkpath, err := curlink.Path(db)
			if err == nil {
				if err = os.Symlink(upath, linkpath); err == nil || os.IsExist(err) {
					err = database.CreateUserLink(db, curlink)
				}
			}
			if err != nil {
				updaterLogger.WithField("user", user.Title()).Warnln("failed to create link for user:", err)
			}
		}
	}()

	if userEntityHeap.Empty() {
		return nil, nil
	}
	log.Debugln("preprocessing finish, elapsed:", time.Since(start))
	log.Debugln("real members:", userEntityHeap.Size())
	log.Debugln("missing tweets:", missingTweets)
	log.Debugln("deepest:", deepest)

	clients := make([]*resty.Client, 0)
	clients = append(clients, client)
	clients = append(clients, additional...)

	fetchStats := make(map[int]*userFetchStat)
	var fetchStatsMtx sync.Mutex

	producer := func(entity *UserEntity) {
		defer prodwg.Done()
		defer panicHandler()

		user := uidToUser[entity.Uid()]
		cli := twitter.SelectUserMediaClient(ctx, clients)
		if ctx.Err() != nil {
			userEntityHeap.Push(entity)
			return
		}
		if cli == nil {
			userEntityHeap.Push(entity)
			cancel(fmt.Errorf("no client available"))
			return
		}

		tweets, err := user.GetMeidas(ctx, cli, &utils.TimeRange{Min: entity.LatestReleaseTime()})
		if err == twitter.ErrWouldBlock {
			userEntityHeap.Push(entity)
			return
		}
		if v, ok := err.(*twitter.TwitterApiError); ok {
			// 客户端不再可用
			if v.Code == twitter.ErrExceedPostLimit {
				twitter.SetClientError(cli, fmt.Errorf("reached the limit for seeing posts today"))
				userEntityHeap.Push(entity)
				return
			} else if v.Code == twitter.ErrAccountLocked {
				twitter.SetClientError(cli, fmt.Errorf("account is locked"))
				userEntityHeap.Push(entity)
				return
			}
		}
		if ctx.Err() != nil {
			userEntityHeap.Push(entity)
			return
		}
		if err != nil {
			// 限流（429 / error code 1015）是临时状态：等待一段时间后重试，
			// 而不是像普通错误那样直接放弃该用户，否则这次运行等于什么都没做。
			if delay, limited := twitter.RateLimitDelay(err); limited {
				rateLimitMtx.Lock()
				retries := rateLimitRetries[entity]
				rateLimitRetries[entity] = retries + 1
				rateLimitMtx.Unlock()

				if retries >= maxRateLimitRetry {
					getterLogger.WithFields(log.Fields{
						"user":    entity.Name(),
						"retries": retries,
					}).Warnln("still rate limited, giving up this run:", err)
					return
				}

				wait := delay
				if wait <= 0 {
					// 没有任何速率头时退避等待，逐步加长
					wait = defaultRateLimitWait * time.Duration(retries+1)
				}
				if wait > maxRateLimitWait {
					wait = maxRateLimitWait
				}

				rateLimitMtx.Lock()
				rateLimitedUsers[entity.Name()] = struct{}{}
				rateLimitMtx.Unlock()

				getterLogger.WithFields(log.Fields{
					"user":    entity.Name(),
					"wait":    wait,
					"attempt": retries + 1,
				}).Warnln("rate limited by x.com, will retry later:", err)

				// 先等待再放回堆：如果立刻放回，调度器会在同一批里再次选中它，
				// 变成并发重复请求，反而加重限流。
				select {
				case <-time.After(wait):
					userEntityHeap.Push(entity)
				case <-ctx.Done():
				}
				return
			}

			getterLogger.WithField("user", entity.Name()).Warnln("failed to get user medias:", err)
			return
		}

		if len(tweets) == 0 {
			rateLimitMtx.Lock()
			delete(rateLimitedUsers, entity.Name())
			rateLimitMtx.Unlock()
			if err := database.UpdateUserEntityMediCount(db, entity.Id(), user.MediaCount); err != nil {
				getterLogger.WithField("user", entity.Name()).Panicln("failed to update user medias count:", err)
			}
			return
		}

		// 成功取到时间线，清掉限流标记
		rateLimitMtx.Lock()
		delete(rateLimitedUsers, entity.Name())
		rateLimitMtx.Unlock()

		// 确保该用户所有推文已推送。注意：这里不再立即推进“最后下载时间”，
		// 而是等所有下载结束后，根据实际成功/失败情况更新（见 finalizeUserFetchStats）。
		sentAll := true
		for _, tw := range tweets {
			pt := TweetInEntity{Tweet: tw, Entity: entity}
			select {
			case tweetChan <- &pt:
			case <-ctx.Done():
				sentAll = false // 防止无消费者导致死锁
			}
			if !sentAll {
				break
			}
		}

		// 只有所有推文都成功进入下载队列且上下文未被取消时才记录进度，
		// 否则一旦进程被中断，未推送的旧推文会因为时间线起点前移而永久丢失。
		if sentAll && ctx.Err() == nil {
			fetchStatsMtx.Lock()
			fetchStats[entity.Id()] = &userFetchStat{
				entity:     entity,
				mediaCount: user.MediaCount,
				tweets:     tweets,
			}
			fetchStatsMtx.Unlock()
		}
	}

	// launch worker
	config := workerConfig{
		ctx:    ctx,
		wg:     &conswg,
		cancel: cancel,
	}
	for i := 0; i < MaxDownloadRoutine; i++ {
		conswg.Add(1)
		go tweetDownloader(client, db, &config, errChan, tweetChan)
	}

	producerPool, err := ants.NewPool(min(userTweetMaxConcurrent, userEntityHeap.Size()))
	if err != nil {
		return nil, err
	}
	defer ants.Release()

	//closer
	go func() {
		// 按批次调用生产者
		for !userEntityHeap.Empty() && ctx.Err() == nil {
			selected := []int{}
			for count := 0; count < userTweetRateLimit && ctx.Err() == nil; {
				if userEntityHeap.Empty() {
					break
				}

				entity := userEntityHeap.Peek()
				depth := depthByEntity[entity]
				if depth > userTweetRateLimit {
					log.WithFields(log.Fields{
						"user":  entity.Name(),
						"depth": depth,
					}).Warnln("user depth greater than the max limit of window")
					userEntityHeap.Pop()
					continue
				}

				if depth+count > userTweetRateLimit {
					break
				}

				prodwg.Add(1)
				producerPool.Submit(func() {
					producer(entity)
				})
				selected = append(selected, depth)

				count += depth
				//delete(depthByEntity, entity)
				userEntityHeap.Pop()
			}
			log.Debugln(selected)
			prodwg.Wait()
		}
		close(tweetChan)
		log.Debugf("getting tweets completed, elapsed time: %v", time.Since(start))

		conswg.Wait()
		close(errChan)
	}()

	fails := []*TweetInEntity{}
	for pt := range errChan {
		fails = append(fails, pt.(*TweetInEntity))
	}
	log.Debugf("%d users unable to start", userEntityHeap.Size())

	rateLimitMtx.Lock()
	limited := len(rateLimitedUsers)
	rateLimitMtx.Unlock()
	if limited > 0 {
		log.WithField("users", limited).Warnln("some users were rate limited by x.com this run; their tweets were not fetched")
	}

	// 下载全部结束后再推进进度：只有成功（或永久跳过）的推文才会被算入，
	// 失败推文下一次仍会落在时间线拉取范围内。
	failedIDs := make(map[uint64]struct{}, len(fails))
	for _, pt := range fails {
		failedIDs[pt.Tweet.Id] = struct{}{}
	}
	if err := finalizeUserFetchStats(db, fetchStats, failedIDs); err != nil {
		getterLogger.Warnln("failed to update user download progress:", err)
	}

	return fails, context.Cause(ctx)
}

// syncListEntity 同步列表实体、文件夹以及身份标记
func syncListEntity(db *sqlx.DB, list twitter.ListBase, dir string) (*ListEntity, error) {
	title := utils.WinFileName(list.Title())
	if marker, _ := database.ReadListMarker(filepath.Join(dir, title)); marker != nil && marker.ListId != list.GetId() {
		title = fmt.Sprintf("%s [%d]", title, list.GetId())
	}

	entity, err := NewListEntity(db, list.GetId(), dir)
	if err != nil {
		return nil, err
	}
	if cur, err := entity.Path(); err == nil {
		if marker, _ := database.ReadListMarker(cur); marker != nil && marker.ListId != list.GetId() {
			if err := entity.RepairName(title); err != nil {
				return nil, err
			}
		}
	}
	if err := syncPath(entity, title); err != nil {
		return nil, err
	}

	path, err := entity.Path()
	if err != nil {
		return nil, err
	}
	if marker, _ := database.ReadListMarker(path); marker != nil && marker.ListId != list.GetId() {
		return nil, fmt.Errorf("folder %s belongs to list %d", path, marker.ListId)
	}
	if err := database.WriteListMarker(path, list.GetId(), list.Title()); err != nil {
		return nil, err
	}
	return entity, nil
}

func downloadList(ctx context.Context, client *resty.Client, db *sqlx.DB, list twitter.ListBase, dir string, realDir string, autoFollow bool, additional []*resty.Client) ([]*TweetInEntity, error) {
	entity, err := syncListEntity(db, list, dir)
	if err != nil {
		return nil, err
	}

	members, err := list.GetMembers(ctx, client)
	if err != nil || len(members) == 0 {
		return nil, err
	}

	eid := entity.Id()
	log.Debugln("members:", len(members))
	packgedUsers := make([]userInLstEntity, len(members))
	for i, user := range members {
		packgedUsers[i] = userInLstEntity{user: user, leid: &eid}
	}
	return BatchUserDownload(ctx, client, db, packgedUsers, realDir, autoFollow, additional)
}

func syncList(db *sqlx.DB, list *twitter.List) error {
	listdb, err := database.GetLst(db, list.Id)
	if err != nil {
		return err
	}
	if listdb == nil {
		return database.CreateLst(db, &database.Lst{Id: list.Id, Name: list.Name, OwnerId: list.Creator.Id})
	}
	return database.UpdateLst(db, &database.Lst{Id: list.Id, Name: list.Name, OwnerId: list.Creator.Id})
}

func DownloadList(ctx context.Context, client *resty.Client, db *sqlx.DB, list twitter.ListBase, dir string, realDir string, autoFollow bool, additional []*resty.Client) ([]*TweetInEntity, error) {
	tlist, ok := list.(*twitter.List)
	if ok {
		if err := syncList(db, tlist); err != nil {
			return nil, err
		}
	}
	return downloadList(ctx, client, db, list, dir, realDir, autoFollow, additional)
}

func syncLstAndGetMembers(ctx context.Context, client *resty.Client, db *sqlx.DB, lst twitter.ListBase, dir string) ([]userInLstEntity, error) {
	if v, ok := lst.(*twitter.List); ok {
		if err := syncList(db, v); err != nil {
			return nil, err
		}
	}

	// update lst path and record
	entity, err := syncListEntity(db, lst, dir)
	if err != nil {
		return nil, err
	}

	// get all members
	members, err := lst.GetMembers(ctx, client)
	if err != nil || len(members) == 0 {
		return nil, err
	}

	// bind lst entity to users for creating symlink
	packgedUsers := make([]userInLstEntity, 0, len(members))
	eid := entity.Id()
	for _, user := range members {
		packgedUsers = append(packgedUsers, userInLstEntity{user: user, leid: &eid})
	}
	return packgedUsers, nil
}

func BatchDownloadAny(ctx context.Context, client *resty.Client, db *sqlx.DB, lists []twitter.ListBase, users []*twitter.User, dir string, realDir string, autoFollow bool, additional []*resty.Client) ([]*TweetInEntity, error) {
	log.Debugln("start collecting users")
	packgedUsers := make([]userInLstEntity, 0)
	wg := sync.WaitGroup{}
	mtx := sync.Mutex{}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	for _, lst := range lists {
		wg.Add(1)
		go func(lst twitter.ListBase) {
			defer wg.Done()
			res, err := syncLstAndGetMembers(ctx, client, db, lst, dir)
			if err != nil {
				cancel(err)
			}
			log.Debugf("members of %s: %d", lst.Title(), len(res))
			mtx.Lock()
			defer mtx.Unlock()
			packgedUsers = append(packgedUsers, res...)
		}(lst)
	}
	wg.Wait()
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}

	for _, usr := range users {
		packgedUsers = append(packgedUsers, userInLstEntity{user: usr, leid: nil})
	}

	log.Debugln("collected users:", len(packgedUsers))
	return BatchUserDownload(ctx, client, db, packgedUsers, realDir, autoFollow, additional)
}

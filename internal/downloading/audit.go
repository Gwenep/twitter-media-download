package downloading

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/jmoiron/sqlx"
	"github.com/Gwenep/twitter-media-download/internal/database"
	"github.com/Gwenep/twitter-media-download/internal/twitter"
	"github.com/Gwenep/twitter-media-download/internal/utils"
)

// mediaCandidate 一条推文里的一个媒体，以及按下载规则它应该叫什么名字。
type mediaCandidate struct {
	Url  string
	Name string
}

// mediaCandidates 按 downloadTweetMedia 的命名规则，列出推文中每个媒体对应的
// 地址和预期文件名。
//
// 下载和核对必须用同一套命名规则，否则核对结果没有意义；
// 因此这里返回 (可用媒体, 无法识别扩展名的地址)。
func mediaCandidates(tweet *twitter.Tweet) ([]mediaCandidate, []string) {
	text := utils.WinFileName(tweet.Text)
	extCount := make(map[string]int)

	candidates := make([]mediaCandidate, 0, len(tweet.Urls))
	skipped := make([]string, 0)
	for _, u := range tweet.Urls {
		if u == "" {
			continue
		}

		ext, err := utils.GetExtFromUrl(u)
		if err != nil || ext == "" {
			skipped = append(skipped, u)
			continue
		}
		ext = strings.ToLower(ext)

		index := extCount[ext]
		extCount[ext] = index + 1
		name := text + ext
		if index > 0 {
			name = fmt.Sprintf("%s(%d)%s", text, index, ext)
		}
		candidates = append(candidates, mediaCandidate{Url: u, Name: name})
	}
	return candidates, skipped
}

// 只读核对（--audit）
//
// `.tmd_media.json` 只能回答“这个本地文件是不是当初下载的那个”，
// 回答不了“远程还有没有没下载的”。后者必须把远程时间线和本地文件
// 逐条比对，因此需要联网。
//
// 核对过程只读：不下载、不写入文件、不修改 media_files，也不推进
// latest_release_time。缺失的条目会打印出来，是否补下由用户决定。

// AuditMissing 一个远程存在、但本地没有对应文件的媒体
type AuditMissing struct {
	TweetId   uint64
	CreatedAt time.Time
	Url       string
	FileName  string
	// Recorded 为 true 表示数据库里有记录，但磁盘上文件不见了
	Recorded bool
}

// AuditResult 单个用户的核对结果
type AuditResult struct {
	IndexTarget
	// EntityId 数据库里对应实体的 id，0 表示数据库中没有该用户
	EntityId int
	// Tweets 远程带媒体的推文数
	Tweets int
	// MediaTotal 远程媒体总数
	MediaTotal int
	// Recorded 通过 media_files 记录核对成功的媒体数
	Recorded int
	// Legacy 没有记录、但按文件名 + mtime / 目录清单认领成功的媒体数
	Legacy int
	// Missing 远程有、本地没有的媒体
	Missing []AuditMissing
	// Extra 本地有、但远程时间线里没有对应上的文件
	Extra []string
	// NoExt 无法从地址识别扩展名的媒体数（无法参与核对）
	NoExt int
}

// AuditUserFolder 拉取用户完整时间线（忽略最新发布时间），逐个媒体核对本地文件。
func AuditUserFolder(ctx context.Context, client *resty.Client, db *sqlx.DB, target IndexTarget) (*AuditResult, error) {
	result := &AuditResult{IndexTarget: target}

	// 找出与文件夹对应的实体记录（只读，不做定位修正）
	if entities, err := database.GetUserEntities(db, target.UserId); err != nil {
		return nil, err
	} else {
		for _, entity := range entities {
			if sameDir(filepath.Join(entity.ParentDir, entity.Name), target.Dir) {
				result.EntityId = int(entity.Id.Int32)
				break
			}
		}
	}

	user := &twitter.User{
		Id:         target.UserId,
		Name:       target.Name,
		ScreenName: target.ScreenName,
	}
	// timeRange 为 nil：获取完整时间线，不受 latest_release_time 影响
	tweets, err := user.GetMeidas(ctx, client, nil)
	if err != nil {
		return nil, err
	}

	used := make(map[string]struct{}, len(tweets))
	for _, tweet := range tweets {
		candidates, skipped := mediaCandidates(tweet)
		result.NoExt += len(skipped)
		if len(candidates) == 0 {
			continue
		}
		result.Tweets++

		for _, media := range candidates {
			result.MediaTotal++

			name, recorded, found := auditMediaFile(db, result.EntityId, target.Dir, tweet, media)
			if found {
				if recorded {
					result.Recorded++
				} else {
					result.Legacy++
				}
				used[name] = struct{}{}
				continue
			}

			result.Missing = append(result.Missing, AuditMissing{
				TweetId:   tweet.Id,
				CreatedAt: tweet.CreatedAt,
				Url:       media.Url,
				FileName:  name,
				Recorded:  recorded,
			})
		}
	}

	// 本地有、远程没有的文件：可能是旧版本产生的重复文件，也可能是推文被删除了
	entries, err := os.ReadDir(target.Dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || name == "" || name[0] == '.' {
			continue
		}
		if _, ok := used[name]; ok {
			continue
		}
		result.Extra = append(result.Extra, name)
	}
	sort.Strings(result.Extra)

	// 时间倒序：最新的缺失排在最前面
	sort.SliceStable(result.Missing, func(i, j int) bool {
		return result.Missing[i].CreatedAt.After(result.Missing[j].CreatedAt)
	})
	return result, nil
}

// auditMediaFile 判断某个远程媒体在本地是否已经有对应文件。
//
// 返回 (文件名, 是否有数据库记录, 是否找到)。
// 判定规则与 prepareMediaFile 完全一致，只是不写任何记录：
//  1. media_files 有记录且文件存在 -> 已下载
//  2. 文件存在、未被其他推文占用，且 mtime 与推文时间一致 -> 旧版本下载的文件
//  3. 文件存在、未被占用，且目录清单里同名文件大小一致 -> 旧版本下载的文件
func auditMediaFile(db *sqlx.DB, entityId int, dir string, tweet *twitter.Tweet, media mediaCandidate) (string, bool, bool) {
	if entityId > 0 {
		if mf, err := database.GetMediaFile(db, tweet.Id, media.Url); err == nil && mf != nil {
			if fileExists(filepath.Join(dir, mf.FileName)) {
				return mf.FileName, true, true
			}
			// 记录在、文件不在：算缺失，文件名以记录为准
			return mf.FileName, true, false
		}
	}

	path := filepath.Join(dir, media.Name)
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return media.Name, false, false
	}

	// 文件名已被其他推文预定/记录时，不能算作这条推文的媒体
	if entityId > 0 {
		if mf, err := database.GetMediaFileByName(db, int32(entityId), media.Name); err != nil || mf != nil {
			return media.Name, false, false
		}
	}

	if sameTweetTime(info.ModTime(), tweet.CreatedAt) {
		return media.Name, false, true
	}
	if entry, ok := database.LookupManifestFile(dir, media.Name); ok &&
		entry.Size == info.Size() && info.Size() > 0 {
		return media.Name, false, true
	}
	return media.Name, false, false
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

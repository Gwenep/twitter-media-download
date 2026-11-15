package downloading

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"github.com/Gwenep/twitter-media-download/internal/database"
)

// 目录索引（--index-only）
//
// 目的：把「这些文件是下载过的」这一事实写进用户文件夹（.tmd_media.json），
// 而不是只存在数据库里。这样即使数据库被删除、换机器重装、或者整个存储目录
// 搬家，只要文件夹还在，旧文件依然能被识别，不会被重新下载一遍。
//
// 整个过程不联网、不下载、不删除任何文件，也不会修改 media_files 记录。

// IndexTarget 一个待建立索引的用户文件夹
type IndexTarget struct {
	UserId     uint64
	Name       string
	ScreenName string
	Dir        string
}

func (t IndexTarget) Title() string {
	if t.ScreenName != "" {
		return fmt.Sprintf("%s(%s)", t.Name, t.ScreenName)
	}
	return t.Name
}

// IndexResult 单个文件夹的索引结果
type IndexResult struct {
	IndexTarget
	// EntityId 数据库里对应实体的 id，0 表示数据库中没有该用户
	EntityId int
	// Total 磁盘上的媒体文件数
	Total int
	// Recorded 磁盘上同时有 media_files 记录的文件数
	Recorded int
	// Unrecorded 磁盘上没有 media_files 记录的文件数
	Unrecorded int
	// Manifested 写入清单的条目数
	Manifested int
	// Added 本次新增的清单条目数（之前没有记录过的文件）
	Added int
	// MissingFiles 数据库有记录、磁盘上却找不到的文件（只报告，不删除记录）
	MissingFiles []string
}

// CollectIndexTargets 扫描 users 目录，返回所有带身份标记的用户文件夹
func CollectIndexTargets(usersDir string) ([]IndexTarget, error) {
	entries, err := os.ReadDir(usersDir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	targets := make([]IndexTarget, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(usersDir, entry.Name())
		marker, err := database.ReadUserMarker(dir)
		if err != nil || marker == nil {
			// 没有标记就无法确认归属，跳过
			continue
		}
		targets = append(targets, IndexTarget{
			UserId:     marker.UserId,
			Name:       marker.Name,
			ScreenName: marker.ScreenName,
			Dir:        dir,
		})
	}
	return targets, nil
}

// ResolveIndexTargets 按 --user 指定的 id / 用户名定位文件夹。
// 只读，不创建数据库记录，也不重命名文件夹。
func ResolveIndexTargets(db *sqlx.DB, usersDir string, ids []uint64, screenNames []string) ([]IndexTarget, error) {
	all, err := CollectIndexTargets(usersDir)
	if err != nil {
		return nil, err
	}

	targets := make([]IndexTarget, 0, len(ids)+len(screenNames))
	for _, id := range ids {
		found := false
		for _, t := range all {
			if t.UserId == id {
				targets = append(targets, t)
				found = true
				break
			}
		}
		if found {
			continue
		}
		target, err := targetFromDB(db, id)
		if err != nil {
			return nil, err
		}
		if target == nil {
			return nil, fmt.Errorf("找不到用户 %d 的下载目录：没有 .tmd_user.json 标记的文件夹，数据库里也没有可用路径", id)
		}
		targets = append(targets, *target)
	}

	for _, screenName := range screenNames {
		found := false
		for _, t := range all {
			if strings.EqualFold(t.ScreenName, screenName) {
				targets = append(targets, t)
				found = true
				break
			}
		}
		if found {
			continue
		}
		usr, err := database.GetUserByScreenName(db, screenName)
		if err != nil {
			return nil, err
		}
		if usr == nil {
			return nil, fmt.Errorf("找不到用户 %s：没有 .tmd_user.json 标记的文件夹，数据库里也没有该用户", screenName)
		}
		target, err := targetFromDB(db, usr.Id)
		if err != nil {
			return nil, err
		}
		if target == nil {
			return nil, fmt.Errorf("找不到用户 %s 的下载目录：数据库有这个用户，但记录的文件夹不存在", screenName)
		}
		targets = append(targets, *target)
	}
	return targets, nil
}

// targetFromDB 用数据库里的实体记录还原一个文件夹（仅当文件夹真实存在且归属一致）
func targetFromDB(db *sqlx.DB, uid uint64) (*IndexTarget, error) {
	entities, err := database.GetUserEntities(db, uid)
	if err != nil {
		return nil, err
	}
	for _, entity := range entities {
		dir := filepath.Join(entity.ParentDir, entity.Name)
		info, err := os.Stat(dir)
		if err != nil || !info.IsDir() {
			continue
		}

		target := IndexTarget{UserId: uid, Name: entity.Name, Dir: dir}
		if marker, _ := database.ReadUserMarker(dir); marker != nil {
			if marker.UserId != uid {
				// 标记显示这个文件夹属于别人，绝不能拿来当当前用户的目录
				continue
			}
			target.Name = marker.Name
			target.ScreenName = marker.ScreenName
		}
		return &target, nil
	}
	return nil, nil
}

// IndexUserFolder 扫描一个用户文件夹，生成/刷新 .tmd_media.json 并返回统计
func IndexUserFolder(db *sqlx.DB, target IndexTarget) (*IndexResult, error) {
	result := &IndexResult{IndexTarget: target}

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

	recorded := make(map[string]struct{})
	if result.EntityId > 0 {
		files, err := database.ListMediaFiles(db, result.EntityId)
		if err != nil {
			return nil, err
		}
		for _, file := range files {
			recorded[file.FileName] = struct{}{}
		}
	}

	entries, err := os.ReadDir(target.Dir)
	if err != nil {
		return nil, err
	}

	files := make([]database.MediaManifestEntry, 0, len(entries))
	onDisk := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		// 跳过子目录和 .tmd_user.json / .tmd_media.json 这类隐藏文件
		if entry.IsDir() || name == "" || name[0] == '.' {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		files = append(files, database.MediaManifestEntry{
			FileName: name,
			Size:     info.Size(),
			ModTime:  info.ModTime(),
		})
		onDisk[name] = struct{}{}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].FileName < files[j].FileName })
	result.Total = len(files)

	for name := range onDisk {
		if _, ok := recorded[name]; ok {
			result.Recorded++
		} else {
			result.Unrecorded++
		}
	}
	for name := range recorded {
		if _, ok := onDisk[name]; !ok {
			result.MissingFiles = append(result.MissingFiles, name)
		}
	}
	sort.Strings(result.MissingFiles)

	old, err := database.ReadMediaManifest(target.Dir)
	if err != nil {
		return nil, err
	}
	known := make(map[string]struct{})
	if old != nil {
		for _, file := range old.Files {
			known[file.FileName] = struct{}{}
		}
	}
	for _, file := range files {
		if _, ok := known[file.FileName]; !ok {
			result.Added++
		}
	}

	if err := database.WriteMediaManifest(target.Dir, target.UserId, files); err != nil {
		return nil, err
	}
	result.Manifested = len(files)
	return result, nil
}

// sameDir 比较两个路径是否指向同一个文件夹（Windows 下不区分大小写）
func sameDir(lhs, rhs string) bool {
	l, err := filepath.Abs(lhs)
	if err != nil {
		l = lhs
	}
	r, err := filepath.Abs(rhs)
	if err != nil {
		r = rhs
	}
	return strings.EqualFold(filepath.Clean(l), filepath.Clean(r))
}

// RefreshSyncedManifests 在下载结束后刷新本次运行涉及的用户文件夹清单。
// 这样即使以后数据库丢失，刚刚下载的内容也已经被清单记住，不会被重下一遍。
// 刷新失败只记录日志，不影响下载结果。
func RefreshSyncedManifests() {
	syncedUsers.Range(func(key, value any) bool {
		entity, ok := value.(*UserEntity)
		if !ok || entity == nil || entity.db == nil {
			return true
		}

		dir, err := entity.Path()
		if err != nil || dir == "" {
			return true
		}
		// 只处理带身份标记、且标记确实属于该用户的文件夹
		marker, err := database.ReadUserMarker(dir)
		if err != nil || marker == nil || marker.UserId != entity.Uid() {
			return true
		}

		target := IndexTarget{
			UserId:     marker.UserId,
			Name:       marker.Name,
			ScreenName: marker.ScreenName,
			Dir:        dir,
		}
		if _, err := IndexUserFolder(entity.db, target); err != nil {
			log.WithField("user", entity.Name()).Warnln("failed to refresh media manifest:", err)
		}
		return true
	})
}

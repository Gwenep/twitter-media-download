package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Gwenep/twitter-media-download/internal/database"
	"github.com/Gwenep/twitter-media-download/internal/downloading"
	"github.com/Gwenep/twitter-media-download/internal/twitter"
)

// 删除仅作用于当前下载根目录。无 --yes 时只展示计划，不修改下载数据。
func runDeleteUser(paths *storePath, target string, execute bool) error {
	db, err := connectDatabase(paths.db)
	if err != nil {
		return err
	}
	defer db.Close()
	target = strings.TrimPrefix(strings.TrimSpace(target), "@")
	uid, _ := strconv.ParseUint(target, 10, 64)
	if uid == 0 {
		u, err := database.GetUserByScreenName(db, target)
		if err != nil {
			return err
		}
		if u != nil {
			uid = u.Id
		}
	}
	marked, err := downloading.CollectIndexTargets(paths.users)
	if err != nil {
		return err
	}
	if uid == 0 {
		for _, m := range marked {
			if strings.EqualFold(m.ScreenName, target) {
				uid = m.UserId
				break
			}
		}
	}
	if uid == 0 {
		return fmt.Errorf("找不到本地用户 %q", target)
	}
	var all []*database.UserEntity
	if err := db.Select(&all, "SELECT * FROM user_entities"); err != nil {
		return err
	}
	entities := map[int]bool{}
	dirs := map[string]bool{}
	usersDir, _ := filepath.Abs(paths.users)
	addDir := func(dir string) error {
		abs, err := filepath.Abs(dir)
		if err != nil {
			return err
		}
		info, err := os.Lstat(abs)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !strings.EqualFold(filepath.Dir(abs), usersDir) {
			return fmt.Errorf("拒绝删除非当前 users 下的普通用户目录: %s", abs)
		}
		marker, err := database.ReadUserMarker(abs)
		if err != nil {
			return err
		}
		if marker == nil || marker.UserId != uid {
			return fmt.Errorf("目录身份不明确或属于其他用户，拒绝删除: %s", abs)
		}
		for _, other := range all {
			if other.Uid != uid && strings.EqualFold(filepath.Clean(filepath.Join(other.ParentDir, other.Name)), abs) {
				return fmt.Errorf("其他账号也指向此目录，拒绝删除: %s", abs)
			}
		}
		dirs[abs] = true
		return nil
	}
	for _, e := range all {
		if e.Uid == uid {
			entities[int(e.Id.Int32)] = true
			if err := addDir(filepath.Join(e.ParentDir, e.Name)); err != nil {
				return err
			}
		}
	}
	for _, m := range marked {
		if m.UserId == uid {
			if err := addDir(m.Dir); err != nil {
				return err
			}
		}
	}
	var links []*database.UserLink
	if err := db.Select(&links, "SELECT * FROM user_links WHERE user_id=?", uid); err != nil {
		return err
	}
	var linkPaths []string
	for _, link := range links {
		path, err := link.Path(db)
		if err != nil {
			return err
		}
		root, err := filepath.Abs(paths.root)
		if err != nil {
			return err
		}
		absLink, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, absLink)
		if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return fmt.Errorf("列表链接不在当前下载根目录，拒绝删除: %s", path)
		}
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink == 0 {
			return fmt.Errorf("列表路径不是符号链接，拒绝删除: %s", path)
		}
		resolved, err := filepath.EvalSymlinks(path)
		if err == nil {
			found := false
			for dir := range dirs {
				if strings.EqualFold(resolved, dir) {
					found = true
				}
			}
			if !found {
				return fmt.Errorf("列表链接指向其他目录，拒绝删除: %s", path)
			}
		}
		linkPaths = append(linkPaths, path)
	}
	failed := map[int][]*twitter.Tweet{}
	original, err := os.ReadFile(paths.errorj)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if len(original) > 0 {
		if err := json.Unmarshal(original, &failed); err != nil {
			return err
		}
	}
	removed := 0
	for eid, tweets := range failed {
		keep := make([]*twitter.Tweet, 0, len(tweets))
		for _, tw := range tweets {
			// 错误归档的推文以作者为准，保留其他作者的失败记录。
			if tw != nil && ((tw.Creator != nil && tw.Creator.Id == uid) || (tw.Creator == nil && entities[eid])) {
				removed++
				continue
			}
			keep = append(keep, tw)
		}
		if entities[eid] && len(keep) > 0 {
			return fmt.Errorf("该用户实体还挂着其他作者的失败记录，请先修复归属后删除")
		}
		if len(keep) == 0 {
			delete(failed, eid)
		} else {
			failed[eid] = keep
		}
	}
	var mediaCount int
	if err := db.Get(&mediaCount, "SELECT COUNT(*) FROM media_files WHERE user_entity_id IN (SELECT id FROM user_entities WHERE user_id=?)", uid); err != nil {
		return err
	}
	fmt.Printf("删除预览：%s（用户 ID %d）\n实体：%d，媒体记录：%d，失败推文：%d\n", target, uid, len(entities), mediaCount, removed)
	for dir := range dirs {
		fmt.Println("  删除整个目录：", dir)
	}
	for _, path := range linkPaths {
		fmt.Println("  删除列表链接：", path)
	}
	fmt.Println("同时删除该用户的曾用名、列表关联和用户记录。历史日志及启动脚本名单保留。")
	fmt.Println("请先关闭所有下载进程。仅凭身份标记无法识别旧版混放的无记录媒体，请核实整个目录均可删除。")
	if !execute {
		fmt.Printf("当前仅预览。确认后执行：tmd --delete-user %s --yes\n", target)
		return nil
	}

	// 先取得 SQLite 写锁；文件先原地改名暂存，数据库操作失败时可以恢复。
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec("UPDATE users SET name=name WHERE id=?", uid); err != nil {
		return err
	}
	staged := map[string]string{}
	committed := false
	defer func() {
		if !committed {
			if _, ok := staged[paths.errorj]; ok {
				os.Remove(paths.errorj)
			}
			for source, temp := range staged {
				if e := os.Rename(temp, source); e != nil {
					fmt.Fprintf(os.Stderr, "恢复失败，请手动恢复 %s -> %s: %v\n", temp, source, e)
				}
			}
		}
	}()
	stage := func(source string) error {
		f, err := os.CreateTemp(filepath.Dir(source), ".tmd-delete-*")
		if err != nil {
			return err
		}
		temp := f.Name()
		f.Close()
		if err := os.Remove(temp); err != nil {
			return err
		}
		if err := os.Rename(source, temp); err != nil {
			return err
		}
		staged[source] = temp
		return nil
	}
	for dir := range dirs {
		if err := stage(dir); err != nil {
			return err
		}
	}
	for _, path := range linkPaths {
		if err := stage(path); err != nil {
			return err
		}
	}
	statements := []string{
		"DELETE FROM media_files WHERE user_entity_id IN (SELECT id FROM user_entities WHERE user_id=?)",
		"DELETE FROM user_links WHERE user_id=?", "DELETE FROM user_previous_names WHERE uid=?",
		"DELETE FROM user_entities WHERE user_id=?", "DELETE FROM users WHERE id=?",
	}
	for _, stmt := range statements {
		if _, err := tx.Exec(stmt, uid); err != nil {
			return err
		}
	}
	if len(original) > 0 {
		data, err := json.MarshalIndent(failed, "", "    ")
		if err != nil {
			return err
		}
		if err := stage(paths.errorj); err != nil {
			return err
		}
		if err := os.WriteFile(paths.errorj, data, 0600); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		if _, ok := staged[paths.errorj]; ok {
			os.Remove(paths.errorj)
		}
		return err
	}
	committed = true
	for _, temp := range staged {
		if err := os.RemoveAll(temp); err != nil {
			return fmt.Errorf("记录已删除，但暂存数据清理失败，请清理 %s: %w", temp, err)
		}
	}
	fmt.Println("删除完成。请从 tmddown.ps1 的 $users 名单中移除该用户；列表/关注下载也可能再次获取该用户。")
	return nil
}

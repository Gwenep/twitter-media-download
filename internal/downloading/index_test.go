package downloading

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Gwenep/twitter-media-download/internal/database"
	"github.com/Gwenep/twitter-media-download/internal/twitter"
)

// 目录索引应把文件夹里的媒体全部写进 .tmd_media.json，并统计出没有记录的条目。
func TestIndexUserFolderWritesManifest(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "tester(9003)")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}

	record := &database.UserEntity{Uid: 9003, ParentDir: base, Name: "tester(9003)"}
	if err := database.CreateUserEntity(db, record); err != nil {
		t.Fatal(err)
	}
	if err := database.WriteUserMarker(dir, 9003, "tester", "tester"); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(dir, "a.jpg"), []byte("123456"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.mp4"), []byte("12345678"), 0644); err != nil {
		t.Fatal(err)
	}
	// a.jpg 已有下载记录，b.mp4 没有（模拟旧版本下载的文件）
	if err := database.CreateMediaFile(db, &database.MediaFile{
		TweetId:      1,
		Url:          "https://example.com/a.jpg",
		FileName:     "a.jpg",
		UserEntityId: record.Id.Int32,
	}); err != nil {
		t.Fatal(err)
	}

	target := IndexTarget{UserId: 9003, Name: "tester", ScreenName: "tester", Dir: dir}
	result, err := IndexUserFolder(db, target)
	if err != nil {
		t.Fatal(err)
	}
	if result.EntityId != int(record.Id.Int32) {
		t.Fatalf("EntityId=%d, want %d", result.EntityId, record.Id.Int32)
	}
	if result.Total != 2 || result.Recorded != 1 || result.Unrecorded != 1 {
		t.Fatalf("counts: total=%d recorded=%d unrecorded=%d", result.Total, result.Recorded, result.Unrecorded)
	}
	if result.Manifested != 2 || result.Added != 2 {
		t.Fatalf("manifest: manifested=%d added=%d", result.Manifested, result.Added)
	}

	manifest, err := database.ReadMediaManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if manifest == nil || len(manifest.Files) != 2 || manifest.UserId != 9003 {
		t.Fatalf("unexpected manifest: %+v", manifest)
	}

	// 再跑一次不应有新条目
	again, err := IndexUserFolder(db, target)
	if err != nil {
		t.Fatal(err)
	}
	if again.Added != 0 || again.Manifested != 2 {
		t.Fatalf("second run: added=%d manifested=%d", again.Added, again.Manifested)
	}
}

// 记录里的文件在磁盘上不见了，只报告，不删除记录。
func TestIndexUserFolderReportsMissingFiles(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "tester(9004)")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}

	record := &database.UserEntity{Uid: 9004, ParentDir: base, Name: "tester(9004)"}
	if err := database.CreateUserEntity(db, record); err != nil {
		t.Fatal(err)
	}
	if err := database.CreateMediaFile(db, &database.MediaFile{
		TweetId:      2,
		Url:          "https://example.com/gone.jpg",
		FileName:     "gone.jpg",
		UserEntityId: record.Id.Int32,
	}); err != nil {
		t.Fatal(err)
	}

	result, err := IndexUserFolder(db, IndexTarget{UserId: 9004, Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.MissingFiles) != 1 || result.MissingFiles[0] != "gone.jpg" {
		t.Fatalf("missing files: %v", result.MissingFiles)
	}

	mf, err := database.GetMediaFile(db, 2, "https://example.com/gone.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if mf == nil {
		t.Fatal("record must not be deleted")
	}
}

// mtime 被改动过的旧文件，只要目录清单里的大小一致，也应该被认领而不重新下载。
func TestPrepareMediaFileAdoptsIndexedFile(t *testing.T) {
	dir := t.TempDir()
	entity := newTestEntity(t, 9103, dir)
	tweet := &twitter.Tweet{Id: 9103, CreatedAt: time.Now().Add(-time.Hour).Truncate(time.Second)}

	content := []byte("indexed-content")
	path := filepath.Join(dir, "indexed.jpg")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	// 故意把 mtime 改成和推文时间不一致（模拟被同步工具/复制工具改过）
	if err := os.Chtimes(path, time.Time{}, time.Now().Add(-72*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := database.WriteMediaManifest(dir, 9103, []database.MediaManifestEntry{
		{FileName: "indexed.jpg", Size: int64(len(content)), ModTime: time.Now()},
	}); err != nil {
		t.Fatal(err)
	}

	name, exist, reserved, err := prepareMediaFile(db, entity, dir, tweet, "https://example.com/c.jpg", "indexed.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if name != "indexed.jpg" || !exist || reserved {
		t.Fatalf("got name=%s exist=%v reserved=%v, want adoption by manifest", name, exist, reserved)
	}

	mf, err := database.GetMediaFile(db, tweet.Id, "https://example.com/c.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if mf == nil || mf.FileName != "indexed.jpg" {
		t.Fatalf("file was not recorded: %+v", mf)
	}
}

// 清单里大小与磁盘不一致时不能认领，否则可能把别的文件当成已下载。
func TestPrepareMediaFileRejectsIndexedSizeMismatch(t *testing.T) {
	dir := t.TempDir()
	entity := newTestEntity(t, 9104, dir)
	tweet := &twitter.Tweet{Id: 9104, CreatedAt: time.Now().Add(-time.Hour).Truncate(time.Second)}

	path := filepath.Join(dir, "changed.jpg")
	if err := os.WriteFile(path, []byte("current-content"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, time.Time{}, time.Now().Add(-72*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := database.WriteMediaManifest(dir, 9104, []database.MediaManifestEntry{
		{FileName: "changed.jpg", Size: 5, ModTime: time.Now()},
	}); err != nil {
		t.Fatal(err)
	}

	name, exist, reserved, err := prepareMediaFile(db, entity, dir, tweet, "https://example.com/d.jpg", "changed.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if exist {
		t.Fatal("size mismatch must not be treated as downloaded")
	}
	if name != "changed(1).jpg" || !reserved {
		t.Fatalf("got name=%s reserved=%v, want changed(1).jpg reserved", name, reserved)
	}
}

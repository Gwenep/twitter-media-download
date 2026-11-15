package downloading

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Gwenep/twitter-media-download/internal/database"
	"github.com/Gwenep/twitter-media-download/internal/twitter"
)

// 核对和下载必须使用同一套命名规则，否则缺失判断会全部错位。
func TestMediaCandidatesMatchesDownloadNaming(t *testing.T) {
	tweet := &twitter.Tweet{
		Text: "hello",
		Urls: []string{
			"https://pbs.twimg.com/media/a.jpg?name=orig",
			"https://video.twimg.com/b.mp4",
			"https://pbs.twimg.com/media/c.jpg",
			"",
		},
	}

	candidates, skipped := mediaCandidates(tweet)
	if len(skipped) != 0 {
		t.Fatalf("unexpected skipped urls: %v", skipped)
	}

	want := []string{"hello.jpg", "hello.mp4", "hello(1).jpg"}
	if len(candidates) != len(want) {
		t.Fatalf("got %d candidates, want %d", len(candidates), len(want))
	}
	for i, name := range want {
		if candidates[i].Name != name {
			t.Fatalf("candidate %d name=%q, want %q", i, candidates[i].Name, name)
		}
		if candidates[i].Url != tweet.Urls[i] {
			t.Fatalf("candidate %d url=%q, want %q", i, candidates[i].Url, tweet.Urls[i])
		}
	}
}

// 无法识别扩展名的地址不能静默丢弃，要单独统计出来。
func TestMediaCandidatesSkipsUnknownExtension(t *testing.T) {
	tweet := &twitter.Tweet{Text: "hi", Urls: []string{"https://example.com/noext"}}
	candidates, skipped := mediaCandidates(tweet)
	if len(candidates) != 0 || len(skipped) != 1 {
		t.Fatalf("candidates=%d skipped=%d", len(candidates), len(skipped))
	}
}

func TestAuditMediaFile(t *testing.T) {
	dir := t.TempDir()
	entity := newTestEntity(t, 9201, dir)
	tweet := &twitter.Tweet{Id: 9201, CreatedAt: time.Now().Add(-time.Hour).Truncate(time.Second)}
	media := mediaCandidate{Url: "https://example.com/a.jpg", Name: "a.jpg"}

	// 1. 没有记录，磁盘上也没有文件
	name, recorded, found := auditMediaFile(db, entity.Id(), dir, tweet, media)
	if found || recorded || name != "a.jpg" {
		t.Fatalf("missing file: name=%q recorded=%v found=%v", name, recorded, found)
	}

	// 2. 旧版本下载的文件：mtime 与推文时间一致
	path := filepath.Join(dir, "a.jpg")
	if err := os.WriteFile(path, []byte("abc"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, time.Time{}, tweet.CreatedAt); err != nil {
		t.Fatal(err)
	}
	name, recorded, found = auditMediaFile(db, entity.Id(), dir, tweet, media)
	if !found || recorded || name != "a.jpg" {
		t.Fatalf("legacy file: name=%q recorded=%v found=%v", name, recorded, found)
	}

	// 3. 数据库有记录但文件丢失
	lost := mediaCandidate{Url: "https://example.com/lost.jpg", Name: "lost.jpg"}
	if err := database.CreateMediaFile(db, &database.MediaFile{
		TweetId:      9201,
		Url:          lost.Url,
		FileName:     lost.Name,
		UserEntityId: int32(entity.Id()),
	}); err != nil {
		t.Fatal(err)
	}
	name, recorded, found = auditMediaFile(db, entity.Id(), dir, tweet, lost)
	if found || !recorded || name != "lost.jpg" {
		t.Fatalf("recorded but missing: name=%q recorded=%v found=%v", name, recorded, found)
	}
}

// mtime 被改过时，只要目录清单里同名文件大小一致，也应视为已下载。
func TestAuditMediaFileUsesManifest(t *testing.T) {
	dir := t.TempDir()
	entity := newTestEntity(t, 9202, dir)
	tweet := &twitter.Tweet{Id: 9202, CreatedAt: time.Now().Add(-time.Hour).Truncate(time.Second)}
	media := mediaCandidate{Url: "https://example.com/m.jpg", Name: "m.jpg"}

	content := []byte("manifest-content")
	if err := os.WriteFile(filepath.Join(dir, "m.jpg"), content, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filepath.Join(dir, "m.jpg"), time.Time{}, time.Now().Add(-72*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := database.WriteMediaManifest(dir, 9202, []database.MediaManifestEntry{
		{FileName: "m.jpg", Size: int64(len(content)), ModTime: time.Now()},
	}); err != nil {
		t.Fatal(err)
	}

	if _, _, found := auditMediaFile(db, entity.Id(), dir, tweet, media); !found {
		t.Fatal("manifest entry with matching name and size must be treated as downloaded")
	}
}

// 同名文件已被其他推文记录占用时，不能算作当前推文的媒体。
func TestAuditMediaFileRejectsClaimedName(t *testing.T) {
	dir := t.TempDir()
	entity := newTestEntity(t, 9203, dir)
	tweet := &twitter.Tweet{Id: 9203, CreatedAt: time.Now().Add(-time.Hour).Truncate(time.Second)}
	media := mediaCandidate{Url: "https://example.com/shared.jpg", Name: "shared.jpg"}

	path := filepath.Join(dir, "shared.jpg")
	if err := os.WriteFile(path, []byte("abc"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, time.Time{}, tweet.CreatedAt); err != nil {
		t.Fatal(err)
	}
	// 同一文件名已经属于另一条推文
	if err := database.CreateMediaFile(db, &database.MediaFile{
		TweetId:      9999,
		Url:          "https://example.com/other.jpg",
		FileName:     "shared.jpg",
		UserEntityId: int32(entity.Id()),
	}); err != nil {
		t.Fatal(err)
	}

	if _, _, found := auditMediaFile(db, entity.Id(), dir, tweet, media); found {
		t.Fatal("file claimed by another tweet must not be counted")
	}
}

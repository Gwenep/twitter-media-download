package downloading

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Gwenep/twitter-media-download/internal/database"
	"github.com/Gwenep/twitter-media-download/internal/twitter"
)

func newTestEntity(t *testing.T, uid uint64, dir string) *UserEntity {
	t.Helper()
	record := &database.UserEntity{Uid: uid, ParentDir: dir, Name: "tester"}
	if err := database.CreateUserEntity(db, record); err != nil {
		t.Fatal(err)
	}
	return &UserEntity{record: record, db: db, created: true}
}

// 旧版本下载的文件（mtime == 推文时间）应被直接认领，不重新下载。
func TestPrepareMediaFileAdoptsLegacyFile(t *testing.T) {
	dir := t.TempDir()
	entity := newTestEntity(t, 9101, dir)
	tweet := &twitter.Tweet{Id: 9101, CreatedAt: time.Now().Add(-time.Hour).Truncate(time.Second)}

	legacy := filepath.Join(dir, "hello.jpg")
	if err := os.WriteFile(legacy, []byte("legacy"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(legacy, time.Time{}, tweet.CreatedAt); err != nil {
		t.Fatal(err)
	}

	name, exist, reserved, err := prepareMediaFile(db, entity, dir, tweet, "https://example.com/a.jpg", "hello.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if name != "hello.jpg" || !exist || reserved {
		t.Fatalf("got name=%s exist=%v reserved=%v, want legacy adoption", name, exist, reserved)
	}

	mf, err := database.GetMediaFile(db, tweet.Id, "https://example.com/a.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if mf == nil || mf.FileName != "hello.jpg" {
		t.Fatalf("legacy file was not recorded: %+v", mf)
	}
}

// mtime 不匹配的文件不能被误认领，必须换一个文件名，避免覆盖已有内容。
func TestPrepareMediaFileDoesNotOverwriteOtherFile(t *testing.T) {
	dir := t.TempDir()
	entity := newTestEntity(t, 9102, dir)
	tweet := &twitter.Tweet{Id: 9102, CreatedAt: time.Now().Add(-time.Hour).Truncate(time.Second)}

	other := filepath.Join(dir, "hello.jpg")
	if err := os.WriteFile(other, []byte("other tweet"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(other, time.Time{}, time.Now().Add(-24*time.Hour)); err != nil {
		t.Fatal(err)
	}

	name, exist, reserved, err := prepareMediaFile(db, entity, dir, tweet, "https://example.com/b.jpg", "hello.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if exist {
		t.Fatal("must not treat another tweet's file as downloaded")
	}
	if name != "hello(1).jpg" || !reserved {
		t.Fatalf("got name=%s reserved=%v, want hello(1).jpg reserved", name, reserved)
	}

	// 预定的文件名再次查询时应沿用，而不是继续生成新的名字
	again, exist, reserved, err := prepareMediaFile(db, entity, dir, tweet, "https://example.com/b.jpg", "hello.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if again != name || exist || reserved {
		t.Fatalf("second call: name=%s exist=%v reserved=%v", again, exist, reserved)
	}

	content, err := os.ReadFile(other)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "other tweet" {
		t.Fatal("existing file was overwritten")
	}
}

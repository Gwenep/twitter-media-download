package downloading

import (
	"testing"

	"github.com/Gwenep/twitter-media-download/internal/database"
	"github.com/Gwenep/twitter-media-download/internal/twitter"
)

// 旧版本会把所有用户的失败推文都记录在同一个实体下。
// 重试时必须按推文作者纠正目标实体，绝不能继续写进别人的文件夹。
func TestDumperReassignsMismatchedTweet(t *testing.T) {
	dir := t.TempDir()

	wrong := &database.UserEntity{Uid: 7201, ParentDir: dir, Name: "wrong"}
	if err := database.CreateUserEntity(db, wrong); err != nil {
		t.Fatal(err)
	}
	right := &database.UserEntity{Uid: 7202, ParentDir: dir, Name: "right"}
	if err := database.CreateUserEntity(db, right); err != nil {
		t.Fatal(err)
	}

	td := NewDumper()
	td.Push(int(wrong.Id.Int32), &twitter.Tweet{
		Id:      1,
		Text:    "belongs to another user",
		Creator: &twitter.User{Id: 7202, Name: "R", ScreenName: "right"},
	})

	got, err := td.GetTotal(db)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d tweets, want 1", len(got))
	}
	if got[0].Entity.Uid() != 7202 {
		t.Fatalf("tweet was assigned to user %d, want 7202", got[0].Entity.Uid())
	}
}

// 作者对应的实体还不存在时，宁可跳过重试，也不能写进别人的目录。
func TestDumperSkipsUnknownAuthor(t *testing.T) {
	dir := t.TempDir()

	wrong := &database.UserEntity{Uid: 7203, ParentDir: dir, Name: "wrong"}
	if err := database.CreateUserEntity(db, wrong); err != nil {
		t.Fatal(err)
	}

	td := NewDumper()
	td.Push(int(wrong.Id.Int32), &twitter.Tweet{
		Id:      2,
		Text:    "unknown author",
		Creator: &twitter.User{Id: 7204, Name: "X", ScreenName: "unknown"},
	})

	got, err := td.GetTotal(db)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d tweets, want 0", len(got))
	}
}

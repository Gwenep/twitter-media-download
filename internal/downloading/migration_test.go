package downloading

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Gwenep/twitter-media-download/internal/database"
	"github.com/Gwenep/twitter-media-download/internal/twitter"
)

// 旧版本会按文件夹名匹配数据库记录，导致第二个用户被关联到第一个用户的文件夹。
// 升级后即使数据库里已经存在这种错误记录，也绝不能再移动/复用别人的文件夹。
func TestSyncUserRepairsHijackedRecord(t *testing.T) {
	dir := t.TempDir()

	aliceUser := &twitter.User{Id: 101, Name: "Alice", ScreenName: "alice"}
	bobUser := &twitter.User{Id: 102, Name: "Bob", ScreenName: "bob"}
	if err := database.CreateUser(db, &database.User{Id: aliceUser.Id, Name: aliceUser.Name, ScreenName: aliceUser.ScreenName}); err != nil {
		t.Fatal(err)
	}
	if err := database.CreateUser(db, &database.User{Id: bobUser.Id, Name: bobUser.Name, ScreenName: bobUser.ScreenName}); err != nil {
		t.Fatal(err)
	}

	// Alice 的文件夹已经存在并带有身份标记
	aliceDir := filepath.Join(dir, aliceUser.Title())
	if err := os.MkdirAll(aliceDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := database.WriteUserMarker(aliceDir, aliceUser.Id, aliceUser.Name, aliceUser.ScreenName); err != nil {
		t.Fatal(err)
	}
	aliceFile := filepath.Join(aliceDir, "alice.jpg")
	if err := os.WriteFile(aliceFile, []byte("alice"), 0644); err != nil {
		t.Fatal(err)
	}

	// 模拟旧版本留下的错误记录：Bob 的记录指向 Alice 的文件夹
	corrupt := &database.UserEntity{Uid: bobUser.Id, ParentDir: dir, Name: aliceUser.Title()}
	if err := database.CreateUserEntity(db, corrupt); err != nil {
		t.Fatal(err)
	}

	bobEntity, err := syncUserAndEntity(db, bobUser, dir)
	if err != nil {
		t.Fatal(err)
	}

	bobPath, err := bobEntity.Path()
	if err != nil {
		t.Fatal(err)
	}
	wantBobPath := filepath.Join(dir, bobUser.Title())
	if bobPath != wantBobPath {
		t.Fatalf("bob path = %s, want %s", bobPath, wantBobPath)
	}
	if _, err := os.Stat(filepath.Join(dir, bobUser.Title(), database.UserMarkerName)); err != nil {
		t.Fatalf("bob marker missing: %v", err)
	}

	// Alice 的文件夹与文件必须原样保留，标记仍然属于 Alice
	if _, err := os.Stat(aliceFile); err != nil {
		t.Fatalf("alice file was moved or lost: %v", err)
	}
	marker, err := database.ReadUserMarker(aliceDir)
	if err != nil {
		t.Fatal(err)
	}
	if marker == nil || marker.UserId != aliceUser.Id {
		t.Fatalf("alice marker = %+v", marker)
	}

	// Bob 的旧记录应被修正名称，而不是指向 Alice 的文件夹
	repaired, err := database.GetUserEntity(db, int(corrupt.Id.Int32))
	if err != nil {
		t.Fatal(err)
	}
	if repaired == nil || repaired.Name != bobUser.Title() {
		t.Fatalf("corrupt record was not repaired: %+v", repaired)
	}
}

package database

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLocateUserEntityDoesNotHijackOtherUsers(t *testing.T) {
	db := opentmpdb()
	defer db.Close()

	root := t.TempDir()
	users := filepath.Join(root, "users")
	if err := os.MkdirAll(users, 0755); err != nil {
		t.Fatal(err)
	}

	alice := &UserEntity{Uid: 1, ParentDir: users, Name: "Alice(alice)"}
	if err := CreateUserEntity(db, alice); err != nil {
		t.Fatal(err)
	}
	aliceDir := filepath.Join(users, alice.Name)
	if err := os.MkdirAll(aliceDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := WriteUserMarker(aliceDir, alice.Uid, "Alice", "alice"); err != nil {
		t.Fatal(err)
	}

	// 第二个用户不允许复用第一个用户的记录/文件夹
	got, err := LocateUserEntity(db, 2, users)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("locate user 2 returned another user's record: %+v", got)
	}

	bob := &UserEntity{Uid: 2, ParentDir: users, Name: "Bob(bob)"}
	created, err := CreateOrUpdateUserEntityWithPathChange(db, bob, users)
	if err != nil {
		t.Fatal(err)
	}
	if created.Uid != 2 {
		t.Fatalf("created entity uid = %d, want 2", created.Uid)
	}
	if created.Id.Int32 == alice.Id.Int32 {
		t.Fatalf("new user reused alice's entity id %d", created.Id.Int32)
	}
	if created.Path() != filepath.Join(users, "Bob(bob)") {
		t.Fatalf("created entity path = %s", created.Path())
	}

	// 第一个用户的记录必须保持不变
	aliceAgain, err := LocateUserEntity(db, 1, users)
	if err != nil {
		t.Fatal(err)
	}
	if aliceAgain == nil || aliceAgain.Id.Int32 != alice.Id.Int32 || aliceAgain.Name != alice.Name {
		t.Fatalf("alice's record was modified: %+v", aliceAgain)
	}
}

func TestLocateUserEntityAfterRootMoved(t *testing.T) {
	db := opentmpdb()
	defer db.Close()

	oldUsers := filepath.Join(t.TempDir(), "users")
	newUsers := filepath.Join(t.TempDir(), "users")
	if err := os.MkdirAll(oldUsers, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(newUsers, 0755); err != nil {
		t.Fatal(err)
	}

	entity := &UserEntity{Uid: 42, ParentDir: oldUsers, Name: "Carol(carol)"}
	if err := CreateUserEntity(db, entity); err != nil {
		t.Fatal(err)
	}
	oldDir := filepath.Join(oldUsers, entity.Name)
	if err := os.MkdirAll(oldDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := WriteUserMarker(oldDir, entity.Uid, "Carol", "carol"); err != nil {
		t.Fatal(err)
	}

	// 模拟整个存储目录移动到新根目录（旧路径已不存在）
	newDir := filepath.Join(newUsers, entity.Name)
	if err := os.MkdirAll(newDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := WriteUserMarker(newDir, entity.Uid, "Carol", "carol"); err != nil {
		t.Fatal(err)
	}

	got, err := LocateUserEntity(db, entity.Uid, newUsers)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("failed to locate user after root move")
	}
	if got.Id.Int32 != entity.Id.Int32 {
		t.Fatalf("entity id = %d, want %d", got.Id.Int32, entity.Id.Int32)
	}
	if got.ParentDir != newUsers || got.Name != entity.Name {
		t.Fatalf("entity path = %s, want %s", got.Path(), newDir)
	}
}

func TestLocateUserEntityRejectsOtherUsersFolder(t *testing.T) {
	db := opentmpdb()
	defer db.Close()

	oldUsers := filepath.Join(t.TempDir(), "users")
	newUsers := filepath.Join(t.TempDir(), "users")
	os.MkdirAll(oldUsers, 0755)
	os.MkdirAll(newUsers, 0755)

	entity := &UserEntity{Uid: 7, ParentDir: oldUsers, Name: "Dave(dave)"}
	if err := CreateUserEntity(db, entity); err != nil {
		t.Fatal(err)
	}
	// 新根目录下同名文件夹明确属于另一个用户
	dir := filepath.Join(newUsers, entity.Name)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := WriteUserMarker(dir, 8, "Other", "other"); err != nil {
		t.Fatal(err)
	}

	got, err := LocateUserEntity(db, entity.Uid, newUsers)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("located a folder belonging to another user: %+v", got)
	}
}

func TestLocateUserEntityLegacyFolderWithoutMarker(t *testing.T) {
	db := opentmpdb()
	defer db.Close()

	oldUsers := filepath.Join(t.TempDir(), "users")
	newUsers := filepath.Join(t.TempDir(), "users")
	os.MkdirAll(oldUsers, 0755)
	os.MkdirAll(newUsers, 0755)

	entity := &UserEntity{Uid: 9, ParentDir: oldUsers, Name: "Eve(eve)"}
	if err := CreateUserEntity(db, entity); err != nil {
		t.Fatal(err)
	}
	// 旧版本没有标记文件，但文件夹随存储目录一起迁移过来了
	if err := os.MkdirAll(filepath.Join(newUsers, entity.Name), 0755); err != nil {
		t.Fatal(err)
	}

	got, err := LocateUserEntity(db, entity.Uid, newUsers)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("failed to adopt legacy folder")
	}
	if got.Id.Int32 != entity.Id.Int32 || got.ParentDir != newUsers {
		t.Fatalf("legacy entity = %+v, want id %d under %s", got, entity.Id.Int32, newUsers)
	}
}

func TestMediaFileMapping(t *testing.T) {
	db := opentmpdb()
	defer db.Close()

	mf := &MediaFile{
		TweetId:      123456789,
		Url:          "https://pbs.twimg.com/media/example.jpg",
		FileName:     "hello.jpg",
		UserEntityId: 5,
	}
	if err := CreateMediaFile(db, mf); err != nil {
		t.Fatal(err)
	}

	got, err := GetMediaFile(db, mf.TweetId, mf.Url)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.FileName != mf.FileName {
		t.Fatalf("GetMediaFile = %+v, want file_name %s", got, mf.FileName)
	}

	// 同一个 (tweet_id, url) 再次写入时更新文件名
	mf.FileName = "hello (1).jpg"
	if err := CreateMediaFile(db, mf); err != nil {
		t.Fatal(err)
	}
	got, err = GetMediaFile(db, mf.TweetId, mf.Url)
	if err != nil {
		t.Fatal(err)
	}
	if got.FileName != mf.FileName {
		t.Fatalf("updated file_name = %s, want %s", got.FileName, mf.FileName)
	}

	byName, err := GetMediaFileByName(db, mf.UserEntityId, mf.FileName)
	if err != nil {
		t.Fatal(err)
	}
	if byName == nil || byName.TweetId != mf.TweetId {
		t.Fatalf("GetMediaFileByName = %+v", byName)
	}

	if err := DeleteMediaFile(db, mf.TweetId, mf.Url); err != nil {
		t.Fatal(err)
	}
	got, err = GetMediaFile(db, mf.TweetId, mf.Url)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("media file still exists after delete: %+v", got)
	}
}

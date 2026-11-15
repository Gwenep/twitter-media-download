package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Gwenep/twitter-media-download/internal/database"
	"github.com/Gwenep/twitter-media-download/internal/twitter"
)

func TestDeleteUserPreviewAndIsolation(t *testing.T) {
	p, err := newStorePath(filepath.Join(t.TempDir(), "downloads"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := connectDatabase(p.db)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, uid := range []uint64{11, 22} {
		name := "target"
		if uid == 22 {
			name = "other"
		}
		dir := filepath.Join(p.users, name)
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := database.WriteUserMarker(dir, uid, name, name); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "video.mp4"), []byte("video"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := database.CreateUser(db, &database.User{Id: uid, Name: name, ScreenName: name}); err != nil {
			t.Fatal(err)
		}
		e := &database.UserEntity{Uid: uid, Name: name, ParentDir: p.users}
		if err := database.CreateUserEntity(db, e); err != nil {
			t.Fatal(err)
		}
		if err := database.CreateMediaFile(db, &database.MediaFile{TweetId: uid, Url: name, FileName: "video.mp4", UserEntityId: e.Id.Int32}); err != nil {
			t.Fatal(err)
		}
	}
	data := []byte(`{"1":[{"Id":11,"Creator":{"Id":11}}],"2":[{"Id":22,"Creator":{"Id":22}}]}`)
	if err := os.WriteFile(p.errorj, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := runDeleteUser(p, "target", false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(p.users, "target", "video.mp4")); err != nil {
		t.Fatal("preview deleted media", err)
	}
	if err := runDeleteUser(p, "target", true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(p.users, "target")); !os.IsNotExist(err) {
		t.Fatal("target directory survived", err)
	}
	if _, err := os.Stat(filepath.Join(p.users, "other", "video.mp4")); err != nil {
		t.Fatal("other media damaged", err)
	}
	u, err := database.GetUserById(db, 11)
	if err != nil || u != nil {
		t.Fatal("target user survived", err)
	}
	u, err = database.GetUserById(db, 22)
	if err != nil || u == nil {
		t.Fatal("other user damaged", err)
	}
	left, err := os.ReadFile(p.errorj)
	if err != nil || string(left) == string(data) {
		t.Fatal("failures not cleaned", err)
	}
	var failures map[int][]*twitter.Tweet
	if err := json.Unmarshal(left, &failures); err != nil {
		t.Fatal(err)
	}
	if len(failures) != 1 || len(failures[2]) != 1 || failures[2][0].Creator.Id != 22 {
		t.Fatal("other user's failure record damaged")
	}
	var count int
	if err := db.Get(&count, "SELECT COUNT(*) FROM media_files"); err != nil || count != 1 {
		t.Fatal("media record isolation failed", err, count)
	}
}

func TestDeleteUserRejectsForeignMarker(t *testing.T) {
	p, err := newStorePath(filepath.Join(t.TempDir(), "downloads"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := connectDatabase(p.db)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	e := &database.UserEntity{Uid: 11, Name: "shared", ParentDir: p.users}
	if err := database.CreateUserEntity(db, e); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(p.users, "shared")
	os.Mkdir(dir, 0700)
	database.WriteUserMarker(dir, 22, "other", "other")
	if err := runDeleteUser(p, "11", true); err == nil {
		t.Fatal("foreign folder accepted")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatal(err)
	}
}

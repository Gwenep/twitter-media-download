package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPortableConfigMigrationDoesNotOverwrite(t *testing.T) {
	exeDir, legacy := t.TempDir(), t.TempDir()
	for _, name := range []string{"conf.yaml", "additional_cookies.yaml"} {
		if err := os.WriteFile(filepath.Join(legacy, name), []byte("old"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	dir, err := prepareConfigDir(exeDir, legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "conf.yaml"), []byte("portable"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareConfigDir(exeDir, legacy); err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]string{
		filepath.Join(dir, "conf.yaml"):               "portable",
		filepath.Join(dir, "additional_cookies.yaml"): "old",
		filepath.Join(legacy, "conf.yaml"):            "old",
	} {
		data, err := os.ReadFile(path)
		if err != nil || string(data) != want {
			t.Fatalf("%s: got %q, err=%v", path, data, err)
		}
	}
}

func TestPortableConfigWithoutHome(t *testing.T) {
	dir, err := prepareConfigDir(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatal(err)
	}
}

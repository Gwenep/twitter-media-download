package main

import (
	"fmt"
	"os"
	"path/filepath"
)

// 配置和日志跟随可执行文件。首次使用时只复制旧配置，不覆盖已有便携配置。
func prepareConfigDir(exeDir, legacyDir string) (string, error) {
	dir := filepath.Join(exeDir, ".tmd-config")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	if _, err := os.Stat(filepath.Join(dir, "conf.yaml")); err == nil {
		return dir, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	for _, name := range []string{"additional_cookies.yaml", "conf.yaml"} {
		target := filepath.Join(dir, name)
		if _, err := os.Stat(target); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			return "", err
		}
		if legacyDir == "" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(legacyDir, name))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", err
		}
		file, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if os.IsExist(err) {
			continue
		}
		if err != nil {
			return "", err
		}
		_, writeErr := file.Write(data)
		closeErr := file.Close()
		if writeErr != nil {
			return "", fmt.Errorf("复制旧配置 %s 失败: %w", name, writeErr)
		}
		if closeErr != nil {
			return "", closeErr
		}
	}
	return dir, nil
}

package database

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// 目录媒体清单
//
// media_files 表记录的是「推文 + 媒体地址 -> 文件名」，但它只是数据库里的一行。
// 删除数据库、换机器重装之后这些记录就没了；而旧版本下载的文件既没有
// media_files 记录，也没有别的标记，只能靠文件名和 mtime 去猜。
//
// 把清单直接写在用户文件夹里（和 .tmd_user.json 一个思路），
// 即使数据库丢失，只要文件夹还在，就能确认「这些文件是下载过的」。
// 清单由 `tmd --index-only` 生成/刷新，正常下载流程只读取它、
// 在 mtime 被改动导致无法按时间认领时作为兜底依据。
const MediaManifestName = ".tmd_media.json"

// MediaManifestEntry 一个媒体文件在清单中的条目。
// Size 用于校验文件没有被替换过；ModTime 记录的是文件当时的 mtime
// （旧版本会把它写成推文发布时间），仅作参考与排查用。
type MediaManifestEntry struct {
	FileName string    `json:"file_name"`
	Size     int64     `json:"size"`
	ModTime  time.Time `json:"mod_time"`
}

type MediaManifest struct {
	UserId    uint64               `json:"user_id"`
	UpdatedAt time.Time            `json:"updated_at"`
	Files     []MediaManifestEntry `json:"files"`
}

// ReadMediaManifest 读取文件夹中的媒体清单，不存在或格式损坏时返回 nil, nil
func ReadMediaManifest(dir string) (*MediaManifest, error) {
	data, err := os.ReadFile(filepath.Join(dir, MediaManifestName))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	manifest := &MediaManifest{}
	if err := json.Unmarshal(data, manifest); err != nil {
		// 清单损坏不应该阻止程序运行，重新生成即可
		return nil, nil
	}
	return manifest, nil
}

// WriteMediaManifest 写入/刷新媒体清单。
// 先写临时文件再改名，避免写到一半被杀导致清单残缺。
func WriteMediaManifest(dir string, uid uint64, files []MediaManifestEntry) error {
	manifest := MediaManifest{
		UserId:    uid,
		UpdatedAt: time.Now(),
		Files:     files,
	}
	data, err := json.MarshalIndent(manifest, "", "    ")
	if err != nil {
		return err
	}

	target := filepath.Join(dir, MediaManifestName)
	tmp := target + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmp, target); err != nil {
		os.Remove(tmp)
		return err
	}

	mediaManifestCache.Delete(manifestKey(dir))
	return nil
}

func manifestKey(dir string) string {
	if abs, err := filepath.Abs(dir); err == nil {
		return abs
	}
	return dir
}

// 进程内缓存：目录 -> 文件名 -> 条目。清单在一次运行中不会变化，
// 而 prepareMediaFile 会被每个媒体调用，不能每次都读磁盘。
var mediaManifestCache sync.Map

func manifestIndex(dir string) map[string]MediaManifestEntry {
	key := manifestKey(dir)
	if v, ok := mediaManifestCache.Load(key); ok {
		return v.(map[string]MediaManifestEntry)
	}

	index := make(map[string]MediaManifestEntry)
	if manifest, _ := ReadMediaManifest(key); manifest != nil {
		for _, entry := range manifest.Files {
			index[entry.FileName] = entry
		}
	}

	actual, _ := mediaManifestCache.LoadOrStore(key, index)
	return actual.(map[string]MediaManifestEntry)
}

// LookupManifestFile 查询目录清单中记录的文件，没有记录时返回 nil, false
func LookupManifestFile(dir string, fileName string) (MediaManifestEntry, bool) {
	entry, ok := manifestIndex(dir)[fileName]
	return entry, ok
}
